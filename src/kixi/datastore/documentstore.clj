(ns kixi.datastore.documentstore)

(defprotocol DocumentStore
  (output-stream [this id]
    "Returns an outputstream for writing a files contents to")
  (retrieve [this meta-data] 
    "Returns an inputstream for read a files contents from"))
