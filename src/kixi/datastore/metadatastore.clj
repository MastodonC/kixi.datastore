(ns kixi.datastore.metadatastore
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore :as schemastore]
            [kixi.datastore.segmentation :as seg]
            [kixi.datastore.schemastore.conformers :as sc]
            [clojure.spec.gen :as gen]))

(s/def ::type #{"stored"})
(s/def ::file-type string?)
(s/def ::id sc/uuid)
(s/def ::parent-id ::id)
(s/def ::pieces-count int?)
(s/def ::name (s/and string?
                     #(re-matches #"^[\p{Digit}\p{IsAlphabetic}].{1,512}[\p{Digit}\p{IsAlphabetic}]$" %)))
(s/def ::description string?)
(s/def ::size-bytes int?)
(s/def ::source #{"upload" "segmentation"})
(s/def ::line-count int?)
(s/def ::header sc/bool?)
(s/def :kixi.user/id sc/uuid)
(s/def ::created sc/timestamp)
(s/def ::added sc/timestamp)

(s/def :kixi.user-group/id sc/uuid)
(s/def :kixi.user/groups (s/coll-of sc/uuid))

(s/def :kixi/user
  (s/keys :req [:kixi.user/id
                :kixi.user/groups]))

(def activities
  [::file-read ::meta-visible ::meta-read ::meta-update])

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

(defmethod file-metadata "stored"
  [_]
  (s/keys :req [::type ::file-type ::id ::name ::provenance ::size-bytes ::sharing]
          :opt [::schema ::segmentations ::segment ::structural-validation ::description]))

(s/def ::file-metadata (s/multi-spec file-metadata ::type))

(s/def ::query-criteria
  (s/keys :req [:kixi.user/groups]
          :opts [::activities]))

(defprotocol MetaDataStore
  (authorised
    [this action id user-groups])
  (exists [this id])
  (retrieve [this id])
  (create-link [this id])
  (query [this criteria from-index count sort-by sort-order]))
