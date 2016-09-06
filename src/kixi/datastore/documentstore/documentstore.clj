(ns kixi.datastore.documentstore.documentstore)

(defprotocol DocumentStore
  (output-stream [this meta-data]
    "Returns an outputstream for writing a files contents to")
  (retrieve [this meta-data] 
    "Returns an inputstream for read a files contents from"))
