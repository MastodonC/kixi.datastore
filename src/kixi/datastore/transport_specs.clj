(ns kixi.datastore.transport-specs
  (:require [clojure
             [spec :as s]
             [walk :as walk]]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]
             [time :as t]]
            [kixi.datastore.schemastore.conformers :refer [uuid]]))

(defmulti file-metadata-transport ::ms/type)

(defmethod file-metadata-transport "stored"
  [_]
  (s/keys :req [::ms/id
                ::ms/name
                ::ms/type
                ::ms/size-bytes
                ::ms/provenance
                ::ms/sharing]
          :opt [::ss/id                
                ::ms/file-type
                ::ms/header]))

(s/def ::file-metadata-transport (s/multi-spec file-metadata-transport ::ms/type))

(def file-type->default-metadata
  {"csv" {::ms/header true}})

(def default-primary-metadata
  {::ms/file-type "csv"})

(s/fdef filemetadata-transport->internal
        :args (s/cat :meta ::file-metadata-transport)
        :fn #(let [{:keys [meta]} (get % :args)
                   file-metadata (get % :ret)]
               (and (= (::ms/id meta)
                       (::ms/id file-metadata))
                    (= (::ms/size-bytes meta)
                       (::ms/size-bytes file-metadata))
                    (= (::ms/provenance meta)
                       (::ms/provenance file-metadata))
                    (= (::ss/id meta)
                       (get-in file-metadata [::ms/schema ::ss/id]))
                    (= (::ms/name meta)
                       (::ms/name file-metadata))
                    (= (::ms/sharing meta)
                       (::ms/sharing file-metadata))
                    (or (= (::ms/type meta)
                           (::ms/type file-metadata))
                        (= "stored"
                           (::ms/type file-metadata)))
                    (case (or (::ms/file-type meta) 
                              (::ms/file-type file-metadata)) 
                      "csv" (if-not (nil? (::ms/header meta))
                              (= (::ms/header meta) 
                                 (::ms/header file-metadata))
                              (true? (::ms/header file-metadata)))
                      false)))
        :ret ::ms/file-metadata)

(defn raise-schema
  [md user-id]
  (if (::ss/id md)
    (-> md
        (assoc ::ms/schema
               {::ss/id (::ss/id md)
                :kixi.user/id user-id
                ::ms/added (t/timestamp)})
        (dissoc ::ss/id))
    md))

(defn filemetadata-transport->internal
  [transport]
  (let [mapped transport
        with-primaries (merge default-primary-metadata
                              mapped)
        with-file-type (merge (get file-type->default-metadata
                                   (::ms/file-type with-primaries))
                              with-primaries)
        with-schema (raise-schema with-file-type 
                                  (get-in transport [::ms/provenance :kixi.user/id]))]
    (prn "WS: " with-schema)
    with-schema))

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
