(ns kixi.datastore.schemastore)

(defprotocol SchemaStore
  (fetch-definition [this spec-name])
  (fetch-spec [this spec-name])
  (persist [this spec-name spec-definition]))
