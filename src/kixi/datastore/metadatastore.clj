(ns kixi.datastore.metadatastore
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore
             [schemastore :as schemastore]
             [segmentation :as seg]]
            [kixi.datastore.metadatastore
             [geography :as geo]
             [license :as l]
             [time :as t]]
            [kixi.datastore.metadatastore.events]
            [kixi.datastore.metadatastore.commands]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.schemastore.utils :as sh]
            [clojure.spec.gen.alpha :as gen]))

(sh/alias 'relaxed 'kixi.datastore.metadatastore.relaxed)

(defn valid-file-name?
  "A file name should be at least one valid character long and only have valid characters and start with a digit or letter."
  [s]
  (when (string? s)
    (re-matches #"^[\p{Digit}\p{IsAlphabetic}].{0,512}$" s)))

(s/def ::type #{"stored" "bundle"})
(s/def ::file-type sc/not-empty-string)
(s/def ::id sc/uuid)
(s/def ::relaxed/id any?)
(s/def ::parent-id ::id)
(s/def ::pieces-count int?)
(s/def ::name (s/with-gen (s/and sc/not-empty-string valid-file-name?)
                #(gen/such-that (fn [x] (and (< 0 (count x) 512)
                                             (re-matches #"^[\p{Digit}\p{IsAlphabetic}]" ((comp str first) x)))) (gen/string) 100)))
(s/def ::description sc/not-empty-string)
(s/def ::logo sc/url)
(s/def ::size-bytes int?)
(s/def ::source #{"upload" "segmentation"})
(s/def ::line-count int?)
(s/def ::header sc/bool?)
(s/def :kixi.user/id sc/uuid)
(s/def ::created sc/timestamp)
(s/def ::added sc/timestamp)
(s/def ::source-created sc/date)
(s/def ::source-updated sc/date)

(s/def ::maintainer sc/not-empty-string)
(s/def ::author sc/not-empty-string)
(s/def ::source sc/not-empty-string)

(s/def :kixi.user-group/id sc/uuid)
(s/def :kixi.group/id sc/uuid)
(s/def :kixi.user/groups (s/coll-of sc/uuid))

(s/def ::bundle-type #{"datapack"})
(s/def ::bundled-ids (s/coll-of sc/uuid :kind set?))
(s/def ::relaxed/bundled-ids (s/coll-of any?))

(s/def :kixi/user
  (s/keys :req [:kixi.user/id
                :kixi.user/groups]))

(def activities
  [::file-read ::meta-visible ::meta-read ::meta-update
   ::bundle-add])

(s/def ::activities
  (s/coll-of (set activities)))

(s/def ::activity
  (set activities))

(s/def ::sharing
  (s/map-of (set activities)
            (s/coll-of :kixi.user-group/id)))

(s/def :kixi.datastore.request/type #{::seg/group-rows-by-column})

(defmulti request :kixi.datastore.request/type)
(defmethod request ::seg/group-rows-by-column
  [_]
  (s/keys :req [::seg/id ::id ::seg/column-name]))

(s/def :kixi.datastore.request/request
  (s/multi-spec request :kixi.datastore.request/type))

(defmulti provenance-type ::source)

(defmethod provenance-type "upload"
  [_]
  (s/keys :req [::source :kixi.user/id ::created]))

(defmethod provenance-type "segmentation"
  [_]
  (s/keys :req [::source ::parent-id :kixi.user/id ::created]))

(s/def ::provenance (s/multi-spec provenance-type ::source))

(defmulti segment-type ::seg/type)

(defmethod segment-type ::seg/group-rows-by-column
  [_]
  (s/keys :req [::seg/type :kixi.datastore.request/request ::seg/line-count ::seg/value]))

(s/def ::segment (s/multi-spec segment-type ::seg/type))

(s/def ::segmentation
  (s/keys :req [:kixi.datastore.request/request ::seg/created]
          :opt [::seg/error ::seg/segment-ids]))

(s/def ::segmentations
  (s/cat :segmentations (s/+ ::segmentation)))

(s/def ::valid boolean?)

(s/def ::spec-explain
  (s/keys))

(s/def ::explain
  (s/cat :errors (s/+ ::spec-explain)))

(s/def ::structural-validation
  (s/keys :req [::valid]
          :opt [::explain]))

(s/def ::schema
  (s/keys :req [::schemastore/id :kixi.user/id ::added]))

(defmulti file-metadata ::type)

(s/def ::tags
  (s/coll-of sc/not-empty-string :kind set))

(defmethod file-metadata "stored"
  [_]
  (s/keys :req [::type ::file-type ::id ::name ::provenance ::size-bytes ::sharing]
          :opt [::schema ::segmentations ::segment ::structural-validation ::description ::logo
                ::tags ::geo/geography ::t/temporal-coverage
                ::maintainer ::author ::source ::l/license
                ::source-created ::source-updated]))

(defmulti bundle-metadata ::bundle-type)

(defmethod bundle-metadata "datapack"
  [_]
  (s/keys :req [::type ::id ::name ::provenance ::sharing ::bundled-ids ::bundle-type]
          :opt [::description ::logo
                ::tags ::geo/geography ::t/temporal-coverage
                ::maintainer ::author ::source ::l/license]))

(defmethod file-metadata "bundle"
  [_]
  (s/multi-spec bundle-metadata ::bundle-type))

(s/def ::file-metadata (s/multi-spec file-metadata ::type))

(s/def ::query-criteria
  (s/keys :req [:kixi.user/groups]
          :opts [::activities]))

(s/def ::sharing-update #{::sharing-conj ::sharing-disj})

(s/def ::sharing-change-payload
  (s/keys :req [::id ::sharing-update :kixi.group/id ::activity]))

(defprotocol MetaDataStore
  (authorised
    [this action id user-groups])
  (exists [this id])
  (retrieve [this id])
  (create-link [this id])
  (query [this criteria from-index count sort-by sort-order]))

(s/def ::metadatastore
  (let [ex #(ex-info "Use stubbed fn version." {:fn %})]
    (s/with-gen
      (partial satisfies? MetaDataStore)
      #(gen/return (reify MetaDataStore
                     (authorised [this action id user-groups] (throw (ex "auth")))
                     (exists [this id] (throw (ex "exists")))
                     (retrieve [this id] (throw (ex "retrieve")))
                     (create-link [this id] (throw (ex "link")))
                     (query [this criteria from-index count sort-by sort-order] (throw (ex "query"))))))))

(s/fdef authorised-fn
        :args (s/cat :impl ::metadatastore
                     :action ::activity
                     :id ::id
                     :user-groups :kixi.user/groups)
        :ret (s/or :nil nil? :set set?))

(defn authorised-fn
  [impl action id user-groups]
  (authorised impl action id user-groups))

(s/fdef retrieve-fn
        :args (s/cat :impl ::metadatastore
                     :id ::id)
        :ret (s/or :nil nil? :metadata ::file-metadata))

(defn retrieve-fn
  [impl id]
  (retrieve impl id))
