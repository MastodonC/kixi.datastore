(ns kixi.datastore.metadatastore)

(defprotocol MetaDataStore
  (fetch [this id])
  (query [this criteria]))
