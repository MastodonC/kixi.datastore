(ns kixi.datastore.metadata-creator
  (:require [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore
             [communication-specs :as cs]
             [filestore :as fs]
             [metadatastore :as ms]
             [time :as t]
             [transport-specs :as ts]]
            [kixi.datastore.schemastore :as ss]))

(defn reject
  [metadata reason] 
  {:kixi.comms.event/key :kixi.datastore.filestore/file-metadata-rejected
   :kixi.comms.event/version "1.0.0"
   :kixi.comms.event/payload {::rejection-reason reason
                              ::ms/file-metadata metadata}})

(defn create-metadata-handler
  [filestore schemastore]
  (fn [{:keys [kixi.comms.command/payload] :as cmd}]
    (let [metadata (ts/filemetadata-transport->internal
                    (assoc-in payload 
                              [::ms/provenance ::ms/created]
                              (t/timestamp)))
          id (::ms/id metadata)
          size-expected (::ms/size-bytes metadata)
          size-actual (fs/size filestore id)
          schema-id (get-in metadata [::ms/schema ::ss/id])]
      (cond
        (nil? size-actual) (reject metadata :file-not-exist)
        (not= size-actual size-expected) (reject metadata :file-size-incorrect)
        (and schema-id
             (not (ss/exists schemastore schema-id))) (reject metadata :schema-unknown)
        :default [{:kixi.comms.event/key :kixi.datastore/file-created
                   :kixi.comms.event/version "1.0.0"
                   :kixi.comms.event/payload metadata}
                  {:kixi.comms.event/key :kixi.datastore/file-metadata-updated
                   :kixi.comms.event/version "1.0.0"
                   :kixi.comms.event/payload {::ms/file-metadata metadata
                                              ::cs/file-metadata-update-type
                                              ::cs/file-metadata-created}}]))))

(defrecord MetadataCreator
    [communications filestore schemastore handler]
    component/Lifecycle
    (start [component]
      (if-not handler
        (assoc component
               :handler
               (c/attach-command-handler!
                communications
                :kixi.datastore/filestore-create-metadata
                :kixi.datastore.filestore/create-file-metadata
                "1.0.0" (create-metadata-handler filestore
                                                 schemastore)))
        component))
    (stop [component]
      (if handler
        (do
          (c/detach-handler! communications handler)
          (dissoc component :handler))
        component)))
