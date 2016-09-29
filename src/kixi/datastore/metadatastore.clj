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



(s/def ::provanance-upload
  (s/keys :req-un [::source ::pieces-count]))

(s/def ::provanance-segment
  (s/keys :req [::source ::line-count ::seg/request ::parent-id]))

;multispec?
(s/def ::provanance
  (s/or ::provanace-upload #(= "upload" (::source %))
        ::provanance-segment #(= "segmentation" (::source %))))

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

(s/def ::filemetadata
  (s/keys :req [::type ::id ::name ::schemastore/id ::provanance ::size-bytes]
          :opt [::segmentations ::segment]))

(defprotocol MetaDataStore
  (fetch [this id])
  (query [this criteria]))
