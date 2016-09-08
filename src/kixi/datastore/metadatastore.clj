(ns kixi.datastore.metadatastore)

(defrecord DocumentMetaData
    [type id pieces-count name size-bytes])

(defprotocol MetaDataStore
  (fetch [this id])
  (query [this criteria]))
