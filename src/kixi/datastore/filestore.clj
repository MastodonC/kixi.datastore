(ns kixi.datastore.filestore
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.filestore.commands]
            [kixi.datastore.filestore.events]
            [kixi.comms :as comms]))

(s/def ::id sc/uuid)
(s/def ::upload-link sc/not-empty-string)
(s/def ::link sc/not-empty-string)

(defmethod comms/command-type->event-types
  [:kixi.datastore.filestore/create-multi-part-upload-link "1.0.0"]
  [_]
  #{[:kixi.datastore.filestore/multi-part-upload-links-created "1.0.0"]})

(defmethod comms/command-type->event-types
  [:kixi.datastore.filestore/complete-multi-part-upload "1.0.0"]
  [_]
  #{[:kixi.datastore.filestore/multi-part-upload-completed "1.0.0"]})

(defprotocol FileStore
  (exists [this id]
    "Checks if there is a file with this id in the store")
  (size [this id]
    "Size in bytes of the file with this id, nil if file does not exist")
  (retrieve [this id]
    "Returns an inputstream for read a files contents from")
  (create-link [this id file-name]
    "Returns a link from which the file can be downloaded"))
