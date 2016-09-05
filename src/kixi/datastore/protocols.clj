(ns kixi.datastore.protocols)

(defprotocol DocumentStore
  (output-stream [this meta-data])
  (retrieve [this meta-data]))

(defprotocol MetaDataStore
  (fetch [this id])
  (query [this criteria]))

(defprotocol Communications
  (new-metadata [this meta-data])
  (attach-new-metadata-processor [this processor])
  (update-metadata [this meta-update])
  (attach-update-metadata-processor [this processor]))

(defprotocol SchemaExtracter)
