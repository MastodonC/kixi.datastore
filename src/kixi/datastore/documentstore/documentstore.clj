(ns kixi.datastore.documentstore.documentstore)

(defprotocol DocumentStore
  (output-stream [this meta-data])
  (retrieve [this meta-data]))
