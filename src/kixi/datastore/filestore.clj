(ns kixi.datastore.filestore
  (:require [clojure.spec :as s]
            [kixi.comms :as c]))

(s/def ::id string?)

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defprotocol FileStore
  (exists [this id]
    "Checks if there is a file with this id in the store")
  (size [this id]
    "Size in bytes of the file with this id, nil if file does not exist")
  (retrieve [this id]
    "Returns an inputstream for read a files contents from"))

(defn create-upload-cmd-handler
  [link-fn]
  (fn [e]
    (let [id (uuid)]
      {:kixi.comms.event/key :kixi.datastore.filestore/upload-link-created
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/payload {::upload-link (link-fn id)
                                  ::id id
                                  :kixi.user/id (get-in e [:kixi.comms.command/user :kixi.user/id])}})))

(defn attach-command-handlers
  [comms {:keys [link-creator
                 file-checker
                 file-size-checker]}]
  (c/attach-command-handler!
   comms
   :kixi.datastore/filestore
   :kixi.datastore.filestore/create-upload-link
   "1.0.0" (create-upload-cmd-handler link-creator)))
