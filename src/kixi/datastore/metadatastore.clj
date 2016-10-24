(ns kixi.datastore.metadatastore
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore :as schemastore]
            [kixi.datastore.segmentation :as seg]))

(s/def ::type #{"csv"})
(s/def ::id string?)
(s/def ::parent-id ::id)
(s/def ::pieces-count int?)
(s/def ::name string?)
(s/def ::size-bytes int?)
(s/def ::source #{"upload" "segmentation"})
(s/def ::line-count int?)

(s/def :kixi.datastore.request/type #{::seg/group-rows-by-column})

(defmulti request :kixi.datastore.request/type)

(defmethod request ::seg/group-rows-by-column
  [_]
  (s/keys :req [::seg/id ::id ::seg/column-name]))

(s/def :kixi.datastore.request/request
  (s/multi-spec request :kixi.datastore.request/type))


(s/def ::provenance-upload
  (s/keys :req-un [::source ::pieces-count]))

(s/def ::provenance-segment
  (s/keys :req [::source ::line-count ::seg/request ::parent-id]))

;multispec?
(s/def ::provenance
  (s/or ::provanace-upload #(= "upload" (::source %))
        ::provenance-segment #(= "segmentation" (::source %))))

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

(defmulti file-metadata-updated-type ::update-type)

(defmethod file-metadata-updated-type ::file-metadata-created
  [_]
  (s/keys :req [::update-type ::file-metadata]))

(defmethod file-metadata-updated-type ::file-metadata-segmentation-add
  [_]
  (s/keys :req [::update-type ::segment]))


(defmethod file-metadata-updated-type ::file-metadata-structual-validation-checked
  [_]
  (s/keys :req [::update-type ::structual-validation ::id]))

(s/def ::file-metadata-updated (s/multi-spec file-metadata-updated-type ::update-type))

(defprotocol MetaDataStore
  (exists [this id])
  (fetch [this id])
  (query [this criteria]))
