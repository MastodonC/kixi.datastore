(ns kixi.datastore.dynamodb
  (:require [environ.core :refer [env]]
            [com.rpl.specter :as sp]
            [joplin.repl :as jrepl]
            [medley.core :refer [map-keys map-vals remove-vals]]
            [taoensso
             [faraday :as far]
             [timbre :as timbre :refer [info]]]
            [kixi.datastore.metadatastore :as md]))

(defn migrate
  [env migration-conf]
  (->>
   (with-out-str
     (jrepl/migrate migration-conf env))
   (clojure.string/split-lines)
   (run! #(info "JOPLIN:" %))))

(def map-depth-seperator "|")
(def namespace-seperator "_")

(defn flat-kw
  [k]
  (if-let [ns (namespace k)]
          (str ns namespace-seperator (name k))
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
       (interpose map-depth-seperator)
       (apply str)))

(defn inflate-kw
  [^String s]
  (if-let [dex (clojure.string/index-of s namespace-seperator)]
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
                (str prefix (flat-kw k) map-depth-seperator)
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

(def serialize (comp remove-empty-str-vals flatten-map))

(defn split-to-kws
  ([s]
   (split-to-kws s 0))
  ([^String s dex]
   (if-let [nxt (clojure.string/index-of s map-depth-seperator dex)]
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
  (far/delete-item conn table pks))

(defn get-item
  ([conn table id-column id]
   (get-item conn table id-column id nil))
  ([conn table id-column id options]
   (let [result (if options
                  (far/get-item conn table {id-column id} 
                                (options->db-opts options))
                  (far/get-item conn table {id-column id}
                                {:consistent? true}))]
     (inflate-map
      (map-keys name result)))))

(defn get-bulk
  [conn table pk-col ids]
  (->> (far/batch-get-item conn
                           {table {:prim-kvs {pk-col ids}
                                   :consistent? true}})
       vals 
       first
       (mapv (comp inflate-map (partial map-keys name)))))

(defn get-bulk-ordered
  [conn table pk-col ids]
  (let [raw-results (get-bulk conn table pk-col ids)
        useable-pk (inflate-kw pk-col)
        pk->r (reduce
               (fn [acc r]
                 (assoc acc (get r useable-pk) r))
               {}
               raw-results)]
    (map (partial get pk->r) ids)))

(defn query
  [conn table pks projection]
  (->> (far/query conn table 
                  (map-vals #(vector "eq" %) pks)
                  {:return (doall (mapv dynamo-col projection))
                   :consistent? true})
       (map (comp inflate-map (partial map-keys name)))))

(defn query-index
  [conn table index pks projection sort-order]
  (->> (far/query conn table 
                  (map-vals #(vector "eq" %) pks)
                  {:return (doall (mapv dynamo-col projection))
                   :consistent? true
                   :order sort-order
                   :index index})
       (map (comp inflate-map (partial map-keys name)))))

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

(def fn-specifier->dynamo-expr
  {:set ["SET" " = "]
   :conj ["ADD" " "]
   :disj ["DELETE" " "]})

(defn update-set
  [conn table id-column id fn-specifier route val]
  (let [raw-attribute-name (dynamo-col route)
        valid-attribute-name (validify-name raw-attribute-name)]
    (far/update-item conn table
                     {id-column id}
                     {:update-expr (str (first (fn-specifier fn-specifier->dynamo-expr)) " "
                                        (second (fn-specifier fn-specifier->dynamo-expr))
                                        valid-attribute-name
                                        " :v")
                      :expr-attr-names {valid-attribute-name raw-attribute-name}
                      :expr-attr-vals {":v" #{val}}})))

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

(defn remove-update-from-namespace
  [data]
  (letfn [(clean-ns [n]
            (if-let [update-dex (and (namespace n) (clojure.string/index-of (namespace n) ".update"))]
               (keyword (subs (namespace n) 0 update-dex) (name n))
               n))]
    (->> data
         (sp/transform
          [sp/MAP-KEYS]
          clean-ns)
         (sp/transform
          [sp/MAP-VALS map? sp/MAP-KEYS]
          clean-ns))))

(defn action-map?
  [m]
  (and (map? m)
       (every? (set (keys fn-specifier->dynamo-expr))
               (keys m))))

(defn update-map?
  [m]
  (and (map? m)
       (every? #{:update-expr :expr-attr-names :expr-attr-vals}
               (keys m))))

(defn expr-attribute-value-generator
  "Sequence of two char keywords for use in update expressions as place holders"
  []
  (let [alphabet (map (comp str char) (range 97 123))]
    (for [a alphabet
          b alphabet]
      (keyword (str a b)))))

(defn combine-update-expr
  [e1 e2]
  (str e1 " " e2))

(sp/defcollector putseqval
  [seq-atom]
  (collect-val 
   [this structure]
   (let [v (first @seq-atom)]
     (swap! seq-atom rest)
     v)))

(def COLLECT-MAP-KEY-DESCEND-MAP-VALS
  (sp/path sp/ALL (sp/collect sp/FIRST) sp/LAST))

(defn create-update-expression
  ([fdepth sdepth operand expr-val-name value]
   (create-update-expression
    (vec (concat fdepth sdepth))
    operand expr-val-name value))
  ([field-path [operand] expr-val-name value]
   (let [raw-attribute-name (dynamo-col field-path)
         valid-attribute-name (validify-name raw-attribute-name)]
     {:update-expr {operand [(str
                               valid-attribute-name
                               (second (operand fn-specifier->dynamo-expr))
                               expr-val-name)]}
      :expr-attr-names {valid-attribute-name raw-attribute-name}
      :expr-attr-vals {(str expr-val-name) value}})))

(defn merge-updates
  [m1 m2]
  (merge-with
   (fn [f s]
     (if (vector? f)
       (vec (concat f s))
       s))
   m1 m2))

(defn concat-update-expr
  [operand->exprs]
  (apply str 
         (interpose " "
                    (reduce-kv
                     (fn [acc op exprs]
                       (cons (apply str
                                    (first (op fn-specifier->dynamo-expr))
                                    " "
                                    (interpose ", " exprs)) 
                             acc))
                     []
                     operand->exprs))))

(defn prn-t
  [x]
  (prn "XXXXX: " x)
  x)

(defn update-data-map->dynamo-update
  [data]
  (let [expr-val-names (atom (expr-attribute-value-generator))]
    (->> data
         remove-update-from-namespace
         (sp/transform
          [COLLECT-MAP-KEY-DESCEND-MAP-VALS
           (sp/if-path action-map?
                       [sp/STAY]
                       COLLECT-MAP-KEY-DESCEND-MAP-VALS)
           COLLECT-MAP-KEY-DESCEND-MAP-VALS (putseqval expr-val-names)]
          create-update-expression)
         (sp/select (sp/walker update-map?))
         (apply merge-with merge-updates)
         (#(update % :update-expr concat-update-expr)))))

(defn update-data
  [conn table id-column id data]
  (far/update-item conn table
                   {id-column id}
                   (prn-t (update-data-map->dynamo-update data))))

