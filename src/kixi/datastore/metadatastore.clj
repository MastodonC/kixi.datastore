(ns kixi.datastore.metadatastore
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore :as schemastore]
            [kixi.datastore.segmentation :as seg]
            [kixi.datastore.schemastore.conformers :as sc]))

(s/def ::type #{"csv"})
(s/def ::id string?)
(s/def ::parent-id ::id)
(s/def ::pieces-count int?)
(s/def ::name string?)
(s/def ::size-bytes int?)
(s/def ::source #{"upload" "segmentation"})
(s/def ::line-count int?)
(s/def ::header sc/bool?)

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
  (s/keys :req [::source ::pieces-count]))

(defmethod provenance-type "segmentation"
  [_]
  (s/keys :req [::source ::line-count ::seg/request ::parent-id]))

(s/def ::provenance (s/multi-spec provenance-type ::source))

(defmulti segment-type ::seg/type)

(defmethod segment-type ::seg/group-rows-by-column
  [_]
  (s/keys :req [::seg/type :kixi.datastore.request/request ::seg/line-count ::seg/value]))

(s/def ::segment (s/multi-spec segment-type ::seg/type))

(s/def ::segmentation
  (s/keys :req [:kixi.datastore.request/request ::seg/created]
          :opt [::seg/msg ::seg/segment-ids]))

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

(s/def ::filemetadata
  (s/keys :req [::type ::id ::name ::schemastore/id ::provenance ::size-bytes]
          :opt [::segmentations ::segment ::structural-validation]))

(defmulti file-metadata-updated-type ::file-metadata-update-type)

(defmethod file-metadata-updated-type ::file-metadata-created
  [_]
  (s/keys :req [::file-metadata-update-type ::file-metadata]))

(defmethod file-metadata-updated-type ::file-metadata-segmentation-add
  [_]
  (s/keys :req [::file-metadata-update-type ::segmentation]))


(defmethod file-metadata-updated-type ::file-metadata-structural-validation-checked
  [_]
  (s/keys :req [::file-metadata-update-type ::structural-validation ::id]))

(s/def ::file-metadata-updated (s/multi-spec file-metadata-updated-type ::file-metadata-update-type))

(defprotocol MetaDataStore
  (exists [this id])
  (fetch [this id])
  (query [this criteria]))
