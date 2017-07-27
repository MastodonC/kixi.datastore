(ns kixi.datastore.metadatastore.command-handler
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
            [kixi.datastore.schemastore :as ss]
            [clojure.spec :as s]))

(sh/alias 'ke 'kixi.comms.event)
(sh/alias 'kc 'kixi.comms.command)
(sh/alias 'mdu 'kixi.datastore.metadatastore.update)
(sh/alias 'kdm 'kixi.datastore.metadatastore)
(sh/alias 'kdfm 'kixi.datastore.file-metadata)


(sh/alias 'event 'kixi.event)

(defn reject
  ([metadata reason]
   {::ke/key ::kdfm/rejected
    ::ke/version "1.0.0"
    ::ke/partition-key (::ms/id metadata)
    ::ke/payload {:reason reason
                  ::ms/file-metadata metadata}})
  ([metadata reason explain]
   {::ke/key ::kdfm/rejected
    ::ke/version "1.0.0"
    ::ke/partition-key (::ms/id metadata)
    ::ke/payload {:reason reason
                  :explaination explain
                  ::ms/file-metadata metadata}})
  ([metadata reason actual expected]
   {::ke/key ::kdfm/rejected
    ::ke/version "1.0.0"
    ::ke/partition-key (::ms/id metadata)
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
   ::ke/partition-key (or (::ms/id payload)
                          (get-in payload [:original ::ms/id]))
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
   ::ke/payload payload
   ::ke/partition-key (or (::ms/id payload)
                          (get-in payload [:original ::ms/id]))})

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
   ::ke/partition-key (::ms/id payload)
   ::ke/payload payload})

(defn get-user-groups
  [cmd]
  (or (get-in cmd [::kc/user :kixi.user/groups])
      (get-in cmd [:kixi/user :kixi.user/groups])))

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
                           (unauthorised-ids metadatastore user-groups (::ms/bundled-ids metadata)))]
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
                   ::ms/description #{:set :rm}
                   ::ms/source #{:set :rm}
                   ::ms/author #{:set :rm}
                   ::ms/maintainer #{:set :rm}
                   ::ms/source-created #{:set :rm}
                   ::ms/source-updated #{:set :rm}
                   ::ms/tags #{:conj :disj}
                   ::l/license {::l/usage #{:set :rm}
                                ::l/type #{:set :rm}}
                   ::geo/geography {::geo/level #{:set :rm}
                                    ::geo/type #{:set :rm}}
                   ::mdt/temporal-coverage {::mdt/from #{:set :rm}
                                            ::mdt/to #{:set :rm}}}

   ["bundle" "datapack"] {::ms/name #{:set}
                          ::ms/description #{:set :rm}
                          ::ms/source #{:set :rm}
                          ::ms/author #{:set :rm}
                          ::ms/maintainer #{:set :rm}
                          ::ms/tags #{:conj :disj}
                          ::l/license {::l/usage #{:set :rm}
                                       ::l/type #{:set :rm}}
                          ::geo/geography {::geo/level #{:set :rm}
                                           ::geo/type #{:set :rm}}
                          ::mdt/temporal-coverage {::mdt/from #{:set :rm}
                                                 ::mdt/to #{:set :rm}}}})

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

(spec/def ::mdu/metadata-update
  (spec/multi-spec metadata-update ::mdu/metadata-update))

(defn structurally-valid
  [metadatastore {:keys [::ms/id] :as payload}]
  (spec/valid? ::mdu/metadata-update
               payload))

(defn not-semantically-valid
  [metadatastore cmd typed-payload]
  (when (= "bundle"
           (::ms/type typed-payload))
    (when-let [unauthed-ids (unauthorised-ids metadatastore (get-user-groups cmd)
                                              (concat (get-in typed-payload [::mdu/bundled-ids :conj])
                                                      (get-in typed-payload [::mdu/bundled-ids :disj])))]
      (invalid cmd {::ms/type "bundle" 
                    :unauthorised-bundled-ids unauthed-ids}))))

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
                     (not (structurally-valid metadatastore typed-payload)) (invalid cmd ::mdu/metadata-update typed-payload)
                     :default (if-let [invalid-event (not-semantically-valid metadatastore cmd typed-payload)] 
                                invalid-event
                                (updated (assoc (dissoc-types typed-payload)
                                                ::cs/file-metadata-update-type ::cs/file-metadata-update
                                                :kixi/user (::kc/user cmd))))))))))

(defmethod c/command-type->event-types
  [:kixi.datastore/delete-bundle "1.0.0"]
  [_]
  #{[:kixi.datastore/bundle-deleted "1.0.0"]
    [:kixi.datastore/bundle-delete-rejected "1.0.0"]})

(defn invalid-bundle-delete
  ([cmd reason id]
   [{::event/type :kixi.datastore/bundle-delete-rejected
     ::event/version "1.0.0"
     :reason reason
     ::ms/id id}
    {:partition-key id}])
  ([cmd reason id explain]
   [{::event/type :kixi.datastore/bundle-delete-rejected
     ::event/version "1.0.0"
     :reason reason
     ::ms/id id
     :spec-explain explain}
    {:partition-key id}]))

(defn bundle?
  [metadatastore id]
  (let [md (ms/retrieve metadatastore id)]
    (= "bundle"
       (::ms/type md))))

(defn create-delete-bundle-handler
  [metadatastore]
  (let [authorised (partial ms/authorised metadatastore ::ms/meta-update)]
    (fn [{:keys [::ms/id] :as cmd}]
      (cond
        (not (spec/valid? :kixi/command cmd)) (invalid-bundle-delete cmd :invalid-cmd id (s/explain-data :kixi/command cmd))
        (not (authorised id (get-user-groups cmd))) (invalid-bundle-delete cmd :unauthorised id)
        (not (bundle? metadatastore id)) (invalid-bundle-delete cmd :incorrect-type id)
        :default [{::event/type :kixi.datastore/bundle-deleted
                   ::event/version "1.0.0"
                   ::ms/id id}
                  {:partition-key id}]))))


(defmethod c/command-type->event-types
  [:kixi.datastore/add-files-to-bundle "1.0.0"]
  [_]
  #{[:kixi.datastore/files-added-to-bundle "1.0.0"]
    [:kixi.datastore/files-add-to-bundle-rejected "1.0.0"]})

(defn reject-add-files-to-bundle
  ([cmd reason id bundled-ids]
   [{::event/type :kixi.datastore/files-add-to-bundle-rejected
     ::event/version "1.0.0"
     :reason reason
     ::ms/id id
     ::ms/bundled-ids bundled-ids}
    {:partition-key id}])
  ([cmd reason id bundled-ids explain]
   [{::event/type :kixi.datastore/files-add-to-bundle-rejected
     ::event/version "1.0.0"
     :reason reason
     ::ms/id id
     ::ms/bundled-ids bundled-ids
     :spec-explain explain}
    {:partition-key id}]))

(defn create-add-files-to-bundle-handler
  [metadatastore]
  (let [authorised (partial ms/authorised metadatastore ::ms/meta-update)]
    (fn [{:keys [::ms/id ::ms/bundled-ids] :as cmd}]
      (cond
        (not (spec/valid? :kixi/command cmd)) (reject-add-files-to-bundle cmd :invalid-cmd id bundled-ids (s/explain-data :kixi/command cmd))
        (not (authorised id (get-user-groups cmd))) (reject-add-files-to-bundle cmd :unauthorised id bundled-ids)
        (not (bundle? metadatastore id)) (reject-add-files-to-bundle cmd :incorrect-type id bundled-ids)
        :default [{::event/type :kixi.datastore/files-added-to-bundle
                   ::event/version "1.0.0"
                   ::ms/id id
                   ::ms/bundled-ids bundled-ids}
                  {:partition-key id}]))))

(defn detach-handlers
  [communications component & handler-kws]
  (reduce
   (fn [c h-kw]
     (update c h-kw
             #(when %
                (c/detach-handler! communications %)
                nil)))   
   component
   handler-kws))

(defrecord MetadataCreator
    [communications filestore schemastore metadatastore
     metadata-create-handler sharing-change-handler
     metadata-update-handler bundle-create-handler
     delete-bundle-handler add-files-to-bundle-handler]
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
             (when-not bundle-create-handler
               {:datapack-create-handler
                (c/attach-command-handler!
                 communications
                 :kixi.datastore/datapack-creator
                 :kixi.datastore/create-datapack
                 "1.0.0" (partial metadata-handler metadatastore
                                  filestore
                                  schemastore))})
             (when-not delete-bundle-handler
               {:delete-bundle-handler
                (c/attach-validating-command-handler!
                 communications
                 :kixi.datastore/bundle-deleter
                 :kixi.datastore/delete-bundle "1.0.0"
                 (create-delete-bundle-handler metadatastore))})
             (when-not add-files-to-bundle-handler
               {:add-files-to-bundle-handler
                (c/attach-validating-command-handler!
                 communications
                 :kixi.datastore/add-files-to-bundler
                 :kixi.datastore/add-files-to-bundle "1.0.0"
                 (create-add-files-to-bundle-handler metadatastore))})
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
      (detach-handlers communications
                       component
                       :metadata-create-handler
                       :datapack-create-handler
                       :bundle-delete-handler
                       :add-files-to-bundle-handler
                       :sharing-change-handler
                       :metadata-update-handler)))