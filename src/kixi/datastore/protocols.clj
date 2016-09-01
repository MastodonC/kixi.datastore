(ns kixi.datastore.protocols)

(defprotocol DocumentStore
  (output-stream [this meta-data])
  (retrieve [this meta-data]))

(defprotocol MetaDataStore
  (store [this meta-data])
  (query [this criteria]))
