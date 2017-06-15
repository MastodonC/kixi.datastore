(ns kixi.datastore.metadatastore.command.handler
  (:require [com.stuartsierra.component :as component]
            [clojure.spec :as spec]
            [com.gfredericks.schpec :as sh]
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
            [kixi.datastore.schemastore :as ss]))

(sh/alias 'ke 'kixi.comms.event)
(sh/alias 'kc 'kixi.comms.command)
(sh/alias 'mdu 'kixi.datastore.metadatastore.update)
(sh/alias 'kdm 'kixi.datastore.metadatastore)
(sh/alias 'kdfm 'kixi.datastore.file-metadata)

(defn reject
  ([metadata reason]
   {::ke/key ::kdfm/rejected
    ::ke/version "1.0.0"
    ::ke/payload {:reason reason
                  ::ms/file-metadata metadata}})
  ([metadata reason explain]
   {::ke/key ::kdfm/rejected
    ::ke/version "1.0.0"
    ::ke/payload {:reason reason
                  :explaination explain
                  ::ms/file-metadata metadata}})
  ([metadata reason actual expected]
   {::ke/key ::kdfm/rejected
    ::ke/version "1.0.0"
    ::ke/payload {:reason reason
                  :actual actual
                  :expected expected
                  ::ms/file-metadata metadata}}))

(spec/def ::sharing-change-cmd-payload
  (spec/keys :req [::ms/id ::ms/sharing-update :kixi.group/id ::ms/activity]))

(defn sharing-change-rejected
  [payload]
  {::ke/key ::kdm/sharing-change-rejected
   ::ke/version "1.0.0"
   ::ke/payload payload})

(defn sharing-change-invalid
  [{:keys [::kc/payload] :as cmd}]
  (sharing-change-rejected {:reason :invalid
                            :explanation (spec/explain-data 
                                          ::sharing-change-cmd-payload payload)
                            :original payload
                            :kixi/user (::kc/user cmd)}))

(defn sharing-change-unauthorised
  [{:keys [::kc/payload] :as cmd}]
  (sharing-change-rejected {:reason :unauthorised
                            ::ms/id (::ms/id payload)
                            :kixi/user (::kc/user cmd)}))

(defn update-rejected
  [payload]
  {::ke/key ::kdm/update-rejected
   ::ke/version "1.0.0"
   ::ke/payload payload})

(defn invalid
  ([cmd speccy data]
   (invalid cmd (spec/explain-data speccy data)))
  ([cmd explanation]
   (update-rejected {:reason :invalid
                     :explanation explanation
                     :original {::ms/payload cmd}
                     :kixi/user (::kc/user cmd)})))

(defn unauthorised
  [cmd id]
  (update-rejected {:reason :unauthorised
                    ::ms/id id
                    :kixi/user (::kc/user cmd)}))

(defn updated
  [payload]
  {::ke/key ::kdfm/updated
   ::ke/version "1.0.0"
   ::ke/payload payload})

(defn get-user-groups
  [cmd]
  (get-in cmd [::kc/user :kixi.user/groups]))

(defn get-user-id
  [cmd]
  (get-in cmd [::kc/user :kixi.user/id]))

(defmulti metadata-handler 
  (fn [metadatastore filestore schemastore
       {:keys [::kc/payload] :as cmd}]
    [(::ms/type payload) (::ms/bundle-type payload)]))

(defmethod metadata-handler
  ["stored" nil]
  [metadatastore filestore schemastore
   {:keys [::kc/payload] :as cmd}]
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
      :default [{::ke/key :kixi.datastore.file/created
                 ::ke/version "1.0.0"
                 ::ke/payload metadata}
                (updated {::ms/file-metadata metadata
                          ::cs/file-metadata-update-type
                          ::cs/file-metadata-created})])))

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
   {:keys [::kc/payload] :as cmd}]
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
      :default (updated {::ms/file-metadata metadata
                         ::cs/file-metadata-update-type
                         ::cs/file-metadata-created}))))

(defn create-sharing-change-handler
  [metadatastore]
  (let [authorised (partial ms/authorised metadatastore ::ms/meta-update)]
    (fn [{:keys [::kc/payload] :as cmd}]
      (cond
        (not (spec/valid? ::sharing-change-cmd-payload payload)) (sharing-change-invalid cmd)
        (not (authorised (::ms/id payload) (get-user-groups cmd))) (sharing-change-unauthorised cmd)
        :default (updated (merge {::cs/file-metadata-update-type
                                  ::cs/file-metadata-sharing-updated
                                  :kixi/user (::kc/user cmd)}
                                 payload))))))

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
                   ::ms/source-created #{:set}
                   ::ms/source-updated #{:set}
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
  (let [type-fields (if (nil? (second dispatch-value))
                      [::ms/id ::ms/type]
                      [::ms/id ::ms/type ::ms/bundle-type])]
    `(defmethod metadata-update
       ~dispatch-value
       ~['_]
       (spec/merge (spec/keys :req ~type-fields
                              :opt ~(mapv updates/update-spec-name (keys spec->action)))
                   (spec/map-of ~(into (set type-fields) (mapv updates/update-spec-name (keys spec->action)))
                                any?)))))

(defmacro define-metadata-update-implementations
  []
  `(do
     ~@(map define-metadata-update-implementation metadata-types->field-actions)))

(define-metadata-update-implementations)

(spec/def ::metadata-update
  (spec/multi-spec metadata-update :metadata-update))

(defn structurally-valid
  [metadatastore {:keys [::ms/id] :as payload}]
  (spec/valid? ::metadata-update 
               payload))

(defn not-semantically-valid
  [metadatastore cmd typed-payload]
  (when (= "bundle"
           (::ms/type typed-payload))
    (when-let [unauthed-ids (unauthorised-ids metadatastore (get-user-groups cmd)
                                              (concat (get-in typed-payload [::mdu/packed-ids :conj])
                                                      (get-in typed-payload [::mdu/packed-ids :disj])))]
      (invalid cmd {::ms/type "bundle" 
                    :unauthorised-packed-ids unauthed-ids}))))

(defn get-metadata-types
  [metadatastore id]
  (select-keys (ms/retrieve metadatastore id)
               [::ms/type ::ms/bundle-type]))

(defn dissoc-types
  [md]
  (dissoc md      
          ::ms/type ::ms/bundle-type))

(defn create-metadata-update-handler
  [metadatastore]
  (let [authorised (partial ms/authorised metadatastore ::ms/meta-update)]
    (fn [{{:keys [::ms/id] :as payload} ::kc/payload :as cmd}]
      (cond
        (not (spec/valid? ::ms/id id)) (invalid cmd ::ms/id id)
        (not (authorised id (get-user-groups cmd))) (unauthorised cmd id)
        :default (let [typed-payload (merge payload
                                            (get-metadata-types metadatastore id))]
                   (cond
                     (not (structurally-valid metadatastore typed-payload)) (invalid cmd ::metadata-update typed-payload)
                     :default (if-let [invalid-event (not-semantically-valid metadatastore cmd typed-payload)] 
                                invalid-event
                                (updated (assoc (dissoc-types typed-payload)
                                                ::cs/file-metadata-update-type ::cs/file-metadata-update
                                                :kixi/user (::kc/user cmd))))))))))

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
                 ::kdm/sharing-change
                 "1.0.0" (create-sharing-change-handler metadatastore))})
             (when-not metadata-update-handler
               {:metadata-update-handler
                (c/attach-command-handler!
                 communications
                 :kixi.datastore/metadata-creator-metadata-update
                 ::kdm/update
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
