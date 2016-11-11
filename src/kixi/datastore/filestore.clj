(ns kixi.datastore.filestore
  (:require [clojure.spec :as s]))

(s/def ::id string?)

(defprotocol FileStore
  (exists [this id]
    "Checks if there is a file with this id in the store")
  (output-stream [this id content-length]
    "Returns an outputstream for writing a files contents to")
  (retrieve [this id] 
    "Returns an inputstream for read a files contents from"))
