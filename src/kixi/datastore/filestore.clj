(ns kixi.datastore.filestore
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.filestore.commands]
            [kixi.datastore.filestore.events]
            [kixi.comms :as comms]))

(s/def ::id sc/uuid)

(defmethod comms/command-type->event-types
  [:kixi.datastore.filestore/initiate-file-upload "1.0.0"]
  [_]
  #{[:kixi.datastore.filestore/file-upload-initiated "1.0.0"]
    [:kixi.datastore.filestore/file-upload-failed "1.0.0"]})

(defmethod comms/command-type->event-types
  [:kixi.datastore.filestore/complete-file-upload
   "1.0.0"]
  [_]
  #{[:kixi.datastore.filestore/file-upload-completed "1.0.0"]
    [:kixi.datastore.filestore/file-upload-rejected "1.0.0"]})

(defprotocol FileStore
  (exists [this id]
    "Checks if there is a file with this id in the store")
  (size [this id]
    "Size in bytes of the file with this id, nil if file does not exist")
  (retrieve [this id]
    "Returns an inputstream for read a files contents from")
  (create-link [this id file-name]
    "Returns a link from which the file can be downloaded"))

(defprotocol FileStoreUploadCache
  (get-item [this file-id]
    "Gets an item with this upload ID")
  (put-item! [this file-id mup? user upload-id]
    "Puts an item into the cache"))
