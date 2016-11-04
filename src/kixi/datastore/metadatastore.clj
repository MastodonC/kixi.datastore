(ns kixi.datastore.metadatastore
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore :as schemastore]
            [kixi.datastore.segmentation :as seg]
            [kixi.datastore.schemastore.conformers :as sc]
            [clojure.spec.gen :as gen]))

(s/def ::type #{"csv"})
(s/def ::id sc/uuid)
(s/def ::parent-id ::id)
(s/def ::pieces-count int?)
(s/def ::name string?)
(s/def ::size-bytes int?)
(s/def ::source #{"upload" "segmentation"})
(s/def ::line-count int?)
(s/def ::header sc/bool?)
(s/def :kixi.user/id sc/uuid)

(s/def :kixi.user-group/id sc/uuid)

(def activities
  [:file-read :meta-visible :meta-read :meta-update])

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
  (s/keys :req [::source ::pieces-count :kixi.user/id]))

(defmethod provenance-type "segmentation"
  [_]
  (s/keys :req [::source ::parent-id :kixi.user/id]))

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

(s/def ::file-metadata
  (s/keys :req [::type ::id ::name ::schemastore/id ::provenance ::size-bytes ::sharing]
          :opt [::segmentations ::segment ::structural-validation]))

(defprotocol MetaDataStore
  (authorisation
    [this domain action id user-groups])
  (exists [this id])
  (fetch [this id])
  (query [this criteria]))
