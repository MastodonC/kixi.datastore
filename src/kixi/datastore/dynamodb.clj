(ns kixi.datastore.dynamodb
  (:require [environ.core :refer [env]]
            [com.rpl.specter :as sp]
            [joplin.repl :as jrepl]
            [medley.core :refer [map-keys map-vals remove-vals]]
            [taoensso
             [faraday :as far]
             [timbre :as timbre :refer [info]]]
            [kixi.datastore.metadatastore :as md]
            [taoensso.timbre :as log]))

(def client-kws
  #{:endpoint})

(defn migrate
  [env migration-conf]
  (->>
   (with-out-str
     (jrepl/migrate migration-conf env))
   (clojure.string/split-lines)
   (run! #(info "JOPLIN:" %))))

(def map-depth-separator "|")
(def namespace-separator "_")

(defn slow-scan
  [conn table-name prim-kv & [{:keys [limit verbose?
                                      filter-expr
                                      expr-attr-vals expr-attr-names]
                               :or {limit 5
                                    verbose? false}}]]
  (letfn [(scan [tok]
            (far/scan conn table-name
                      (merge {:limit limit}
                             (when (and filter-expr expr-attr-vals expr-attr-names)
                               {:filter-expr filter-expr
                                :expr-attr-vals expr-attr-vals
                                :expr-attr-names expr-attr-names})
                             (when tok
                               {:last-prim-kvs {prim-kv tok}}))))]
    (loop [out []
           r (scan nil)]
      (when verbose?
        (println "Slow scan:" (count out) "results"))
      (if (empty? r)
        out
        (recur (concat out r) (scan (get (last r) prim-kv)))))))

(defn flat-kw
  [k]
  (if-let [ns (namespace k)]
          (str ns namespace-separator (name k))
          (name k)))

(defn size-1
  [v]
  (= 1 (count v)))

(defmulti dynamo-col
  (fn [target]
    (cond
      (string? target) :str
      (keyword? target) :kw
      (and (vector? target)
           (every? keyword target)) :vec-kws)))

(defmethod dynamo-col :str
  [s] s)

(defmethod dynamo-col :kw
  [kw]
  (flat-kw kw))

(defmethod dynamo-col :vec-kws
  [v]
  (->> v
       (map flat-kw)
       (interpose map-depth-separator)
       (apply str)))

(def dynamo-col-k (comp keyword dynamo-col))

(defn inflate-kw
  [^String s]
  (if-let [dex (clojure.string/index-of s namespace-separator)]
    (keyword (subs s 0 dex)
             (subs s (inc dex)))
    (keyword s)))

(defn flatten-map
  ([m]
   (flatten-map nil m))
  ([prefix m]
   (reduce-kv
    (fn [acc k v]
      (cond
        (and (map? v)
             (not-empty v))
        (merge acc
               (flatten-map
                (str prefix (flat-kw k) map-depth-separator)
                v))
        (and (sequential? v)
             (not-empty v)
             (every? map? v))
        (assoc acc (str prefix (flat-kw k))
               (mapv (partial flatten-map nil) v))
        :else (assoc acc (str prefix (flat-kw k)) v)))
    {}
    m)))

(def remove-empty-str-vals
  (partial remove-vals #(and (string? %) (empty? %))))

(def remove-empty-sets
  (partial remove-vals #(and (set? %) (empty? %))))

(def serialize (comp remove-empty-sets remove-empty-str-vals flatten-map))

(defn split-to-kws
  ([s]
   (split-to-kws s 0))
  ([^String s dex]
   (if-let [nxt (clojure.string/index-of s map-depth-separator dex)]
     (cons (inflate-kw (subs s dex nxt))
           (lazy-seq (split-to-kws s (inc nxt))))
     [(inflate-kw (subs s dex))])))

(defn inflate-map
  [m]
  (reduce-kv
   (fn [acc k v]
     (cond
       (and (sequential? v)
            (every? map? v))
       (assoc-in
        acc
        (split-to-kws k)
        (mapv #(inflate-map (map-keys name %)) v))
       :else (assoc-in
              acc
              (split-to-kws k)
              v)))
   {}
   m))

(defn vec-if-not
  [v]
  (if (vector? v)
    v
    [v]))

(def freeze-column-names
  #{(dynamo-col [:kixi.datastore.metadatastore/structural-validation :kixi.datastore.metadatastore/explain])
    (dynamo-col [:kixi.datastore.metadatastore/segmentations])})

(defn freeze-columns
  [data]
  (reduce
   (fn [acc n]
     (if (get acc n)
       (update acc n far/freeze)
       acc))
   data
   freeze-column-names))

(defn validify-name
  [n]
  (let [^StringBuilder sb (StringBuilder.)]
    (.append sb \#)
    (doseq [^char c n]
      (when-not (#{\- \| \.} c)
        (.append sb c)))
    (str sb)))

(defn projection->proj-expr
  [projection]
  (->> projection
       (map validify-name)
       (interpose ", ")
       (apply str)))

(defn options->db-opts
  [options]
  {:proj-expr (projection->proj-expr (:projection options))
   :expr-attr-names (zipmap (map validify-name (:projection options))
                            (:projection options))
   :consistent? true})

(defn put-item
  [conn table item]
  (far/put-item conn table (serialize item)))

(defn delete-item
  [conn table pks]
  (far/delete-item conn table (map-keys dynamo-col pks)))

(defn get-item-ignore-tombstone
  ([conn table id-column id]
   (get-item-ignore-tombstone conn table id-column id nil))
  ([conn table id-column id options]
   (let [result (if options
                  (far/get-item conn table {id-column id}
                                (options->db-opts (update options :projection
                                                          #(conj % (dynamo-col ::md/tombstone)))))
                  (far/get-item conn table {id-column id}
                                {:consistent? true}))]
     (inflate-map
      (map-keys name result)))))

(defn get-item
  ([conn table id-column id]
   (get-item conn table id-column id nil))
  ([conn table id-column id options]
   (let [result (get-item-ignore-tombstone conn table id-column id options)]
     (when-not (::md/tombstone result)
       result))))

(defn get-bulk
  [conn table pk-col ids]
  (->> (far/batch-get-item conn
                           {table {:prim-kvs {pk-col ids}
                                   :consistent? true}})
       vals
       first
       (mapv (comp inflate-map (partial map-keys name)))
       (remove ::md/tombstone)))

(defn get-bulk-ordered
  [conn table pk-col ids]
  (let [raw-results (get-bulk conn table pk-col ids)
        useable-pk (inflate-kw pk-col)
        pk->r (reduce
               (fn [acc r]
                 (assoc acc (get r useable-pk) r))
               {}
               raw-results)]
    (keep (partial get pk->r) ids)))

(defn query
  ([conn table pks]
   (query conn table pks nil))
  ([conn table pks projection]
   (->> (far/query conn table
                   (map-vals #(vector "eq" %) pks)
                   {:return (if projection
                              (doall (mapv dynamo-col
                                           (conj projection
                                                 ::md/tombstone)))
                              :all-attributes)
                    :consistent? true})
        (map (comp inflate-map (partial map-keys name)))
        (remove ::md/tombstone))))

(defn query-index
  [conn table index pks projection sort-order]
  (->> (far/query conn table
                  (map-vals #(vector "eq" %) pks)
                  {:return (doall (mapv dynamo-col
                                        (conj projection
                                              ::md/tombstone)))
                   :consistent? true
                   :order sort-order
                   :index index})
       (map (comp inflate-map (partial map-keys name)))
       (remove ::md/tombstone)))

(defn insert
  [conn table rows]
  (let [targets (vec-if-not rows)]
    (far/batch-write-item conn
                          {table {:put (mapv serialize targets)}})))

(defn map->update-map
  [data]
  (let [flat (freeze-columns (serialize data))]
    (zipmap (map keyword (keys flat))
            (map #(vector :put %) (vals flat)))))

(defn merge-data
  [conn table id-column id data]
  (far/update-item conn table
                   {id-column id}
                   {:update-map (map->update-map data)}))

(defn insert-data
  [conn table id-column data]
  (far/put-item conn table
                (map-keys keyword (freeze-columns (serialize data)))
                {:cond-expr "attribute_not_exists(#id)"
                 :expr-attr-names {"#id" id-column}}))

(defn delete-data
  [conn table id-column id timestamp]
  (far/update-item conn table
                   {id-column id}
                   {:update-map
                    (map->update-map {::md/tombstone true
                                      ::md/tombstoned-at timestamp})}))

(def operand->dynamo-op
  {:set ["SET " " = "]
   :rm ["REMOVE " ""]
   :conj ["ADD " " "]
   :disj ["DELETE " " "]})

(def operands (set (keys operand->dynamo-op)))

(defn set-if-not
  [v]
  (cond
    (set? v) v
    (coll? v) (set v)
    :default #{v}))

(defn update-set
  [conn table id-column id operand route val]
  (let [raw-attribute-name (dynamo-col route)
        valid-attribute-name (validify-name raw-attribute-name)
        [dynamo-op separator] (operand operand->dynamo-op)]
    (far/update-item conn table
                     {id-column id}
                     {:update-expr (str dynamo-op
                                        separator
                                        valid-attribute-name
                                        " :v")
                      :expr-attr-names {valid-attribute-name raw-attribute-name}
                      :expr-attr-vals {":v" (set-if-not val)}})))

(defn append-list
  [conn table id-column id route val]
  (let [raw-attribute-name (dynamo-col route)
        valid-attribute-name (validify-name raw-attribute-name)]
    (far/update-item conn table
                     {id-column id}
                     {:update-expr (str  "SET "
                                         valid-attribute-name
                                         " = list_append( " valid-attribute-name ", :v)")
                      :expr-attr-names {valid-attribute-name raw-attribute-name}
                      :expr-attr-vals {":v" val}})))

(def expr-attribute-value-generator
  "Sequence of two char keywords for use in update expressions as place holders"
  (let [alphabet (map (comp str char) (range 97 123))]
    (for [a alphabet
          b alphabet]
      (keyword (str a b)))))

(defn combine-update-expr
  [e1 e2]
  (str e1 " " e2))

(defn create-update-expression
  [[field-path operand value expr-val-name]]
  (let [raw-attribute-name (dynamo-col field-path)
        valid-attribute-name (validify-name raw-attribute-name)
        [dynamo-op attr-name-val-sep] (operand operand->dynamo-op)]
    (case operand
      :rm {:update-expr {dynamo-op [valid-attribute-name]}
           :expr-attr-names {valid-attribute-name raw-attribute-name}}
      {:update-expr {dynamo-op [(str
                                 valid-attribute-name
                                 attr-name-val-sep
                                 expr-val-name)]}
       :expr-attr-names {valid-attribute-name raw-attribute-name}
       :expr-attr-vals {(str expr-val-name) value}})))

(defn merge-updates
  [m1 m2]
  (merge-with
   (partial merge-with
     (fn [f s]
       (if (vector? f)
         (vec (concat f s))
         s)))
   m1 m2))

(defn concat-update-expr
  [operand->exprs]
  (->> operand->exprs
       (map
        (fn [[op exprs]]
          (apply str
                 op
                 (interpose ", " exprs))))
       (interpose " ")
       (apply str)))

(defn map->flat-vectors
  [x]
  (if (map? x)
    (vec
     (mapcat
      (fn [[k v]]
        (map
         #(vec (cons k %))
         (map->flat-vectors v)))
      x))
    [[x]]))

(defn vectorise-metadata-path
  [tuple]
  (sp/transform
   (sp/srange-dynamic (constantly 0) #(count (take-while (complement operands) %)))
   vector
   tuple))

(defn remove-update-from-metadata-path
  [tuple]
  (letfn [(clean-ns [kw]
            (let [ns (namespace kw)
                  n (name kw)
                  update-dex (clojure.string/index-of ns ".update")]
               (keyword (subs ns 0 update-dex) n)))]
    (sp/transform
     [sp/FIRST sp/ALL]
     clean-ns
     tuple)))

(defn update-data-map->dynamo-update
  [data]
  (transduce
   (comp
    (map vectorise-metadata-path)
    (map remove-update-from-metadata-path)
    (map create-update-expression))
   (fn reducer
     ([] [{}])
     ([updates expr]
      (letfn [(able-to-merge?
                [base expr]
                (not (get (:expr-attr-names base)
                      (ffirst (:expr-attr-names expr)))))
              (targeted-merge
                [new-updates current-updates expr]
                (if-let [f (first current-updates)]
                  (if (able-to-merge? f expr)
                    (concat new-updates [(merge-updates f expr)] (rest current-updates))
                    (targeted-merge (conj new-updates f) (rest current-updates) expr))
                  (conj new-updates expr)))]
        (targeted-merge [] updates expr)))
     ([updates]
      (mapv
       #(update %
                :update-expr concat-update-expr)
       updates)))
   (map conj
        (map->flat-vectors data)
        expr-attribute-value-generator)))

(defn update-data
  [conn table id-column id data]
  (doseq [update-expr (update-data-map->dynamo-update data)]
      (far/update-item conn table
                       {id-column id}
                       update-expr)))
