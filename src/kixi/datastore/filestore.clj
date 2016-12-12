(ns kixi.datastore.filestore
  (:require [clojure.spec :as s]
            [kixi.comms :as c]
            [kixi.datastore.transport-specs :as ts]
            [kixi.datastore 
             [communication-specs :as cs]]
            [kixi.datastore.time :as t]
            [kixi.datastore.metadatastore :as ms]))

(s/def ::id string?)

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defprotocol FileStore
  (exists [this id]
    "Checks if there is a file with this id in the store")
  (retrieve [this id]
    "Returns an inputstream for read a files contents from"))

(defn create-upload-cmd-handler
  [link-fn]
  (fn [_]
    (let [id (uuid)]
      {:kixi.comms.event/key :kixi.datastore.filestore/upload-link-created
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/payload {::upload-link (link-fn id)
                                  ::id id}})))

(defn create-metadata-handler
  [file-checker]
  (fn [{:keys [kixi.comms.command/payload] :as cmd}]
    (let [metadata (ts/filemetadata-transport->internal
                    (assoc-in payload 
                              [::ms/provenance ::ms/created]
                              (t/timestamp)))]
      (if (file-checker (::ms/id metadata) (::ms/size-bytes metadata))
        {:kixi.comms.event/key :kixi.datastore/file-metadata-updated
         :kixi.comms.event/version "1.0.0"
         :kixi.comms.event/payload {::ms/file-metadata metadata
                                    ::cs/file-metadata-update-type
                                    ::cs/file-metadata-created}}
        {:kixi.comms.event/key :kixi.datastore.filestore/file-metadata-rejected
         :kixi.comms.event/version "1.0.0"
         :kixi.comms.event/payload metadata}))))

(defn attach-command-handlers
  [comms {:keys [link-creator
                 file-checker]}]
  (c/attach-command-handler!
   comms
   :kixi.datastore/filestore
   :kixi.datastore.filestore/create-upload-link
   "1.0.0" (create-upload-cmd-handler link-creator))
  (c/attach-command-handler!
   comms
   :kixi.datastore/filestore-create-metadata
   :kixi.datastore.filestore/create-file-metadata
   "1.0.0" (create-metadata-handler file-checker)))

