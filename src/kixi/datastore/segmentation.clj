(ns kixi.datastore.segmentation
  (:require [clojure.spec :as s]
            [kixi.datastore.filestore :as kdfs]))

(s/def ::id string?)

(s/def ::column-name string?)

(s/def column-segmentation-request
  (s/keys :req [::id :kdfs/id ::column-name]))

(defprotocol SegmentationRequest)

(defrecord ColumnSegmentationRequest
    [id file-id column-name]
  SegmentationRequest)

(defprotocol Segmentation)
