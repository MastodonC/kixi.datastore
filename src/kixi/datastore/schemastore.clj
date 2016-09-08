(ns kixi.datastore.schemastore)

(defprotocol SchemaStore
  (fetch [id]))
