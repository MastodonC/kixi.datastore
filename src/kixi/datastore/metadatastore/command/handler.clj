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
             [time :as mdt]
             [updates :as updates]]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as md]))

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


(defmulti metadata-handler 
  (fn [metadatastore filestore schemastore
       {:keys [kixi.comms.command/payload] :as cmd}]
    [(::ms/type payload) (::ms/bundle-type payload)]))

(defmethod metadata-handler
  ["stored" nil]
  [metadatastore filestore schemastore
   {:keys [kixi.comms.command/payload] :as cmd}]
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
                                            ::cs/file-metadata-created}}])))

(defn unauthorised-ids
  [metadatastore user-groups ids]
  (seq
   (doall
    (remove
     (fn [id]
       (ms/authorised metadatastore ::ms/meta-read id user-groups))
     ids))))

(defmethod metadata-handler
  ["bundle" "datapack"]
  [metadatastore filestore schemastore
   {:keys [kixi.comms.command/payload] :as cmd}]
  (let [metadata (assoc-in payload
                           [::ms/provenance ::ms/created]
                           (t/timestamp))
        user-groups (get-user-groups cmd)
        metadata-explain (spec/explain-data ::ms/file-metadata metadata)
        unauthorised-ids (when-not metadata-explain
                           (unauthorised-ids metadatastore user-groups (::ms/packed-ids metadata)))]
    (cond
      metadata-explain (reject metadata :metadata-invalid metadata-explain)
      unauthorised-ids (reject metadata :unauthorised {:unauthorised-ids unauthorised-ids})
      :default {:kixi.comms.event/key :kixi.datastore.file-metadata/updated
                :kixi.comms.event/version "1.0.0"
                :kixi.comms.event/payload {::ms/file-metadata metadata
                                           ::cs/file-metadata-update-type
                                           ::cs/file-metadata-created}})))

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
                                  :original payload
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
                                  :original payload
                                  :kixi/user (:kixi.comms.command/user cmd)}})))

(defmulti metadata-update
   (fn [payload]
     [(::ms/type payload) (::ms/bundle-type payload)]))

(comment "
This is a declarative method of defining spec's for our update command payloads.

The map defines dispatch values for the metadata-update multimethod, which backs the
::metadata-update multispec. The values of the map define which fields are available for
modification and the actions allowable for the modification.

updates/create-update-specs creates new spec definitions for all the fields specified, with 
an 'update' appended to the namespace. Each is map-spec of the declared actions to a valid
value of the original spec.

defmethod implementations of metadata-update are then created. Defining map spec's using
the generated 'update' specs.
")

(def metadata-types->field-actions
  {["stored" nil] {::ms/name #{:set}
                   ::ms/description #{:set}
                   ::ms/source #{:set}
                   ::ms/author #{:set}
                   ::ms/maintainer #{:set}
                   ::ms/tags #{:conj :disj}
                   ::l/license {::l/usage #{:set}
                                ::l/type #{:set}}
                   ::geo/geography {::geo/level #{:set}
                                    ::geo/type #{:set}}
                   ::mdt/temporal-coverage {::mdt/from #{:set}
                                            ::mdt/to #{:set}}}

   ["bundle" "datapack"] {::ms/name #{:set}
                          ::ms/description #{:set}
                          ::ms/source #{:set}
                          ::ms/author #{:set}
                          ::ms/maintainer #{:set}
                          ::ms/tags #{:conj :disj}
                          ::ms/packed-ids #{:conj :disj}
                          ::l/license {::l/usage #{:set}
                                       ::l/type #{:set}}
                          ::geo/geography {::geo/level #{:set}
                                           ::geo/type #{:set}}
                          ::mdt/temporal-coverage {::mdt/from #{:set}
                                                 ::mdt/to #{:set}}}})

(updates/create-update-specs metadata-types->field-actions)

(defn define-metadata-update-implementation
  [[dispatch-value spec->action]]
  `(defmethod metadata-update
     ~dispatch-value
     ~['_]
     (spec/keys :req ~(if (nil? (second dispatch-value))
                        [::ms/id ::ms/type]
                        [::ms/id ::ms/type ::ms/bundle-type])
                :opt ~(mapv updates/update-spec-name (keys spec->action)))))

(defmacro define-metadata-update-implementations
  []
  `(do
     ~@(map define-metadata-update-implementation metadata-types->field-actions)))

(define-metadata-update-implementations)

(spec/def ::metadata-update
  (spec/multi-spec metadata-update :metadata-update))

(defn invalid
  [cmd speccy data]
  {:kixi.comms.event/key :kixi.datastore.metadatastore/update-rejected
   :kixi.comms.event/version "1.0.0"
   :kixi.comms.event/payload {:reason :invalid
                              :explanation (spec/explain-data speccy data)
                              :original {::ms/payload cmd}
                              :kixi/user (:kixi.comms.command/user cmd)}})

(defn unauthorised
  [cmd id]
  {:kixi.comms.event/key :kixi.datastore.metadatastore/update-rejected
   :kixi.comms.event/version "1.0.0"
   :kixi.comms.event/payload {:reason :unauthorised
                              ::ms/id id
                              :kixi/user (:kixi.comms.command/user cmd)}})

(defn updated
  [payload]
  {:kixi.comms.event/key :kixi.datastore.file-metadata/updated
   :kixi.comms.event/version "1.0.0"
   :kixi.comms.event/payload payload})

(defn assoc-types
  [md payload]
  (merge payload
         (select-keys md
                      [::ms/type ::ms/bundle-type])))

(defn valid-update
  [metadatastore {:keys [::ms/id] :as payload}]
  (spec/valid? ::metadata-update 
               payload))

(defn resolve-packed-ids
  [metadatastore user-groups current updates]
  (if (= "bundle"
         (::ms/type updates))
    (let [current-ids (set (::ms/packed-ids current))
          updated-ids (set (::ms/packed-ids updates))
          removed (clojure.set/difference current-ids updated-ids)
          added (clojure.set/difference updated-ids current-ids)
          removable (apply disj removed (unauthorised-ids metadatastore user-groups removed))
          addable (apply disj added (unauthorised-ids metadatastore user-groups added))]
      (assoc updates
             ::ms/packed-ids
             (->> current-ids
                  (concat addable)
                  (remove (set removable))
                  vec)))
    updates))

(defn create-metadata-update-handler
  [metadatastore]
  (fn [{{:keys [::ms/id] :as payload} :kixi.comms.command/payload :as cmd}]
    (cond
      (not (spec/valid? ::ms/id id)) (invalid cmd ::ms/id id)
      (not (ms/authorised metadatastore ::ms/meta-update id (get-user-groups cmd))) (unauthorised cmd id)
      :default (let [current-meta (ms/retrieve metadatastore id)
                     typed-payload (assoc-types current-meta payload)]
                 (cond
                   (not (valid-update metadatastore typed-payload)) (invalid cmd ::metadata-update typed-payload)
                   :default (let [with-resolved-packed-ids (resolve-packed-ids metadatastore (get-user-groups cmd) current-meta typed-payload)] 
                                (updated (assoc with-resolved-packed-ids
                                                ::cs/file-metadata-update-type ::cs/file-metadata-update
                                                :kixi/user (:kixi.comms.command/user cmd)))))))))

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
                 "1.0.0" (partial metadata-handler metadatastore
                                  filestore
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
