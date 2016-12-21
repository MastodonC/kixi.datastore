(ns kixi.datastore.metadata-creator
  (:require [com.stuartsierra.component :as component]
            [clojure.spec :as spec]
            [kixi.comms :as c]
            [kixi.datastore
             [communication-specs :as cs]
             [filestore :as fs]
             [metadatastore :as ms]
             [time :as t]
             [transport-specs :as ts]]
            [kixi.datastore.schemastore :as ss]))

(defn reject
  ([metadata reason]
   {:kixi.comms.event/key :kixi.datastore.file-metadata/rejected
    :kixi.comms.event/version "1.0.0"
    :kixi.comms.event/payload {:reason reason
                               ::ms/file-metadata metadata}})
([metadata reason explain]
   {:kixi.comms.event/key :kixi.datastore.file-metadata/rejected
    :kixi.comms.event/version "1.0.0"
    :kixi.comms.event/payload {:reason reason
                               :explaination explain
                               ::ms/file-metadata metadata}})
  ([metadata reason actual expected]
   {:kixi.comms.event/key :kixi.datastore.file-metadata/rejected
    :kixi.comms.event/version "1.0.0"
    :kixi.comms.event/payload {:reason reason
                               :actual actual
                               :expected expected
                               ::ms/file-metadata metadata}}))

(defn get-user-groups
  [cmd]
  (get-in cmd [:kixi.comms.command/user :kixi.user/groups]))

(defn create-metadata-handler
  [filestore schemastore]
  (fn [{:keys [kixi.comms.command/payload] :as cmd}]
    (let [metadata (ts/filemetadata-transport->internal
                    (assoc-in payload
                              [::ms/provenance ::ms/created]
                              (t/timestamp)))
          metadata-explain (spec/explain-data ::ms/file-metadata metadata)
          id (::ms/id metadata)
          size-expected (::ms/size-bytes metadata)
          size-actual (fs/size filestore id)
          schema-id (get-in metadata [::ms/schema ::ss/id])
          user-groups (get-user-groups cmd)]
      (cond
        metadata-explain (reject metadata :metadata-invalid metadata-explain)
        (nil? size-actual) (reject metadata :file-not-exist)
        (not= size-actual size-expected) (reject metadata :file-size-incorrect size-actual size-expected)
        (and schema-id
             (not (ss/exists schemastore schema-id))) (reject metadata :schema-unknown)
        (and schema-id
             (not (ss/authorised schemastore ::ss/use schema-id user-groups))) (reject metadata :unauthorised)
        :default [{:kixi.comms.event/key :kixi.datastore.file/created
                   :kixi.comms.event/version "1.0.0"
                   :kixi.comms.event/payload metadata}
                  {:kixi.comms.event/key :kixi.datastore.file-metadata/updated
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
                :kixi.datastore/metadata-creator
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
