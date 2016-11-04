(ns kixi.datastore.transport-specs
  (:require [clojure.spec :as s]
            [clojure.walk :as walk]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.schemastore.conformers :refer [uuid]]))

(s/def ::schema-id uuid)

(s/def ::filemetadata-transport
  (s/keys :req-un [::schema-id
                   ::ms/name]
          :opt-un [::ms/type
                   ::ms/header
                   ::ms/sharing]))

(s/def ::file-details
  (s/keys :req [::ms/id ::ms/size-bytes ::ms/provenance]))

(def file-type->default-metadata
  {"csv" {::ms/header true}})

(def default-primary-metadata
  {::ms/type "csv"
   ::ms/sharing {}})

(s/fdef filemetadata-transport->internal
        :args (s/cat :meta ::filemetadata-transport
                     :details ::file-details)
        :fn #(let [{:keys [meta details]} (get % :args)
                   file-metadata (get % :ret)]
               (and (= (::ms/id details)
                       (::ms/id file-metadata))
                    (= (::ms/size-bytes details)
                       (::ms/size-bytes file-metadata))
                    (= (::ms/provenance details)
                       (::ms/provenance file-metadata))
                    (= (:schema-id meta)
                       (::ss/id file-metadata))
                    (= (:name meta)
                       (::ms/name file-metadata))
                    (or (= (:sharing meta)
                           (::ms/sharing file-metadata))
                        (= {}
                           (::ms/sharing file-metadata)))
                    (case (or (:type meta) 
                              (::ms/type file-metadata)) 
                      "csv" (if-not (nil? (:header meta))
                              (= (:header meta) 
                                 (::ms/header file-metadata))
                              (true? (::ms/header file-metadata)))
                      false)))
        :ret ::ms/file-metadata)

(def key-mapping
  {:name ::ms/name
   :header ::ms/header
   :type ::ms/type
   :schema-id ::ss/id
   :sharing ::ms/sharing})

(defn filemetadata-transport->internal
  [transport file-details]
  (let [mapped (zipmap (map key-mapping (keys transport))
                       (vals transport))
        with-primaries (merge default-primary-metadata
                              mapped)
        with-file-type (merge (get file-type->default-metadata
                                   (::ms/type with-primaries))
                              with-primaries)]
    (merge with-file-type
           file-details)))

(defn add-ns-to-keys
  ([ns m]
   (letfn [(process [n]
             (if (= (type n) clojure.lang.MapEntry)
               (clojure.lang.MapEntry. (keyword (namespace ns) (name (first n))) (second n))
               n))]
     (walk/prewalk process m))))

(defn map-every-nth [f coll n]
  (map-indexed #(if (zero? (mod %1 n)) (f %2) %2) coll))

(defn keywordize-values
  [m]
  (let [keys-values-to-keywordize [::ss/name]
        m' (reduce
            (fn [a k]
              (update a
                      k
                      keyword))
            m
            keys-values-to-keywordize)]
    (case (get-in m' [::ss/schema ::ss/type])
      "list" (update-in m'
                        [::ss/schema ::ss/definition]
                        #(map-every-nth
                          keyword
                          % 2))
      m')))

(s/fdef schema-transport->internal      
        :ret ::ss/create-schema-request)

(defn schema-transport->internal
  [transport]
  (let [schema (keywordize-values
                (add-ns-to-keys ::ss/_ 
                                   transport))]
    schema))

(defn raise-spec
  [conformed]
  (assoc conformed
         ::ss/schema
         (second (::ss/schema conformed))))
