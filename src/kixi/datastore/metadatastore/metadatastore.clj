(ns kixi.datastore.metadatastore.metadatastore)

(defprotocol MetaDataStore
  (fetch [this id])
  (query [this criteria]))
