(ns kixi.datastore.filestore
  (:require [clojure.spec :as s]))

(s/def ::id string?)

(defprotocol FileStore
  (output-stream [this id]
    "Returns an outputstream for writing a files contents to")
  (retrieve [this id] 
    "Returns an inputstream for read a files contents from"))
