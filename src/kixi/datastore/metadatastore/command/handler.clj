(ns kixi.datastore.metadatastore.command.handler
  (:require [com.stuartsierra.component :as component]
            [clojure.spec :as spec]
            [kixi.comms :as c]
            [kixi.datastore
             [communication-specs :as cs]
             [filestore :as fs]
             [metadatastore :as ms]
             [time :as t]
             [transport-specs :as ts]]
            [kixi.datastore.metadatastore
             [geography :as geo]
             [license :as l]
             [time :as mdt]]
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

(defn get-user-id
  [cmd]
  (get-in cmd [:kixi.comms.command/user :kixi.user/id]))

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

(spec/def ::sharing-change-cmd-payload
  (spec/keys :req [::ms/id ::ms/sharing-update :kixi.group/id ::ms/activity]))

(defn create-sharing-change-handler
  [metadatastore]
  (fn [{:keys [kixi.comms.command/payload] :as cmd}]
    (if (spec/valid? ::sharing-change-cmd-payload payload)      
      (let [user-id (get-user-id cmd)
            user-groups (get-user-groups cmd)
            metadata-id (::ms/id payload)]
        (if (ms/authorised metadatastore ::ms/meta-update metadata-id user-groups)
          (let [event-payload (merge {::cs/file-metadata-update-type
                                      ::cs/file-metadata-sharing-updated
                                      :kixi/user (:kixi.comms.command/user cmd)}
                                     payload)]
            {:kixi.comms.event/key :kixi.datastore.file-metadata/updated
             :kixi.comms.event/version "1.0.0"
             :kixi.comms.event/payload event-payload})
          {:kixi.comms.event/key :kixi.datastore.metadatastore/sharing-change-rejected
           :kixi.comms.event/version "1.0.0"
           :kixi.comms.event/payload {:reason :unauthorised
                                      ::ms/id metadata-id
                                      :kixi/user (:kixi.comms.command/user cmd)}}))
      {:kixi.comms.event/key :kixi.datastore.metadatastore/sharing-change-rejected
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/payload {:reason :invalid
                                  :explanation (spec/explain-data ::sharing-change-cmd-payload payload)
                                  :kixi/user (:kixi.comms.command/user cmd)}})))

(spec/def ::metadata-update
  (spec/merge (spec/keys :req [::ms/id]
                         :opt [::ms/name ::ms/description
                               ::ms/tags ::geo/geography ::mdt/temporal-coverage 
                               ::ms/maintainer ::ms/author ::ms/source ::l/license])
              (spec/map-of #{::ms/id ::ms/name ::ms/description
                             ::ms/tags ::geo/geography ::mdt/temporal-coverage 
                             ::ms/maintainer ::ms/author ::ms/source ::l/license}
                           any?)))

(defn create-metadata-update-handler
  [metadatastore]
  (fn [{:keys [kixi.comms.command/payload] :as cmd}]
    (if (spec/valid? ::metadata-update payload)
      (let [user-id (get-user-id cmd)
            user-groups (get-user-groups cmd)
            metadata-id (::ms/id payload)]
        (if (ms/authorised metadatastore ::ms/meta-update metadata-id user-groups)
          (let [event-payload (merge {::cs/file-metadata-update-type
                                      ::cs/file-metadata-update
                                      :kixi/user (:kixi.comms.command/user cmd)}
                                     payload)]
            {:kixi.comms.event/key :kixi.datastore.file-metadata/updated
             :kixi.comms.event/version "1.0.0"
             :kixi.comms.event/payload event-payload})
          {:kixi.comms.event/key :kixi.datastore.metadatastore/update-rejected
           :kixi.comms.event/version "1.0.0"
           :kixi.comms.event/payload {:reason :unauthorised
                                      ::ms/id metadata-id
                                      :kixi/user (:kixi.comms.command/user cmd)}}))
      {:kixi.comms.event/key :kixi.datastore.metadatastore/update-rejected
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/payload {:reason :invalid
                                  :explanation (spec/explain-data ::metadata-update payload)
                                  :kixi/user (:kixi.comms.command/user cmd)}})))

(defrecord MetadataCreator
    [communications filestore schemastore metadatastore
     metadata-create-handler sharing-change-handler metadata-update-handler]
    component/Lifecycle
    (start [component]
      (merge component
             (when-not metadata-create-handler
               {:metadata-create-handler
                (c/attach-command-handler!
                 communications
                 :kixi.datastore/metadata-creator
                 :kixi.datastore.filestore/create-file-metadata
                 "1.0.0" (create-metadata-handler filestore
                                                  schemastore))})
             (when-not sharing-change-handler
               {:sharing-change-handler
                (c/attach-command-handler!
                 communications
                 :kixi.datastore/metadata-creator-sharing-change
                 :kixi.datastore.metadatastore/sharing-change
                 "1.0.0" (create-sharing-change-handler metadatastore))})
             (when-not metadata-update-handler
               {:metadata-update-handler
                (c/attach-command-handler!
                 communications
                 :kixi.datastore/metadata-creator-metadata-update
                 :kixi.datastore.metadatastore/update
                 "1.0.0" (create-metadata-update-handler metadatastore))})))
    (stop [component]
      (-> component
          (update component :metadata-create-handler
                  #(when %
                     (c/detach-handler! communications %)
                     nil))
          (update component :sharing-change-handler
                  #(when %
                     (c/detach-handler! communications %)
                     nil))
          (update component :metadata-update-handler
                  #(when %
                     (c/detach-handler! communications %)
                     nil)))))
