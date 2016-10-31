(ns kixi.datastore.transport-specs
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.schemastore.conformers :refer [uuid]]))

(s/def ::schema-id uuid)

(s/def ::filemetadata-transport
  (s/keys :req-un [::schema-id
                   ::ms/name
                   ::ms/file-sharing
                   ::ms/file-metadata-sharing]
          :opt-un [::ms/type
                   ::ms/header]))

(s/def ::file-details
  (s/keys :req [::ms/id ::ms/size-bytes ::ms/provenance]))

(def file-type->default-metadata
  {"csv" {::ms/header true}})

(def default-primary-metadata
  {::ms/type "csv"})

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
                    (= (:file-sharing meta)
                       (::ms/file-sharing file-metadata))
                    (= (:file-metadata-sharing meta)
                       (::ms/file-metadata-sharing file-metadata))
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
   :file-sharing ::ms/file-sharing
   :file-metadata-sharing ::ms/file-metadata-sharing})

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
