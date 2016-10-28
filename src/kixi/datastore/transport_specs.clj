(ns kixi.datastore.transport-specs
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.schemastore.conformers :refer [uuid]]))

(s/def ::schema-id uuid)

(s/def ::filemetadata-transport
  (s/keys :req-un [::schema-id
                   ::ms/name
                   ::ms/header]
          :opt-un [::ms/type]))

(s/def ::file-details
  (s/keys :req [::ms/id ::ms/size-bytes ::ms/provenance]))

(def file-type->default-metadata
  {"csv" {::ms/headers true}})

(def default-primary-metadata
  {::ms/type "csv"})

(s/fdef filemetadata-transport->internal
        :args (s/cat :meta ::filemetadata-transport
                     :details ::file-details)
        :ret ::ms/file-metadata)

(def key-mapping
  {:name ::ms/name
   :header ::ms/header
   :type ::ms/type
   :schema-id ::ss/id})

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
