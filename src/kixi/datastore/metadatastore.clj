(ns kixi.datastore.metadatastore
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore :as schemastore]))

(s/def ::type #{"csv"})
(s/def ::id string?)
(s/def ::pieces-count int?)
(s/def ::name string?)
(s/def ::size-bytes int?)

(s/def filemetadata
  (s/keys :req [::type ::id ::pieces-count ::name ::size-bytes :schemastore/id]))

(defrecord FileMetaData
    [type id pieces-count name size-bytes schema-id])

(defprotocol MetaDataStore
  (fetch [this id])
  (query [this criteria]))
