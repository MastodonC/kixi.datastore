(ns kixi.datastore.schemastore)

(defrecord Schema
    [])

(defprotocol SchemaStore
  (fetch [id]))
