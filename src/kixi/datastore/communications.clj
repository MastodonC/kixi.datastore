(ns kixi.datastore.communications
  (:require [clojure.spec :as s]))

(s/def ::type #{:segmentation
                :schemastore
                :filestore
                :structural-validation
                :metadatastore})

(defprotocol Communications
  (submit [this msg])
  (attach-pipeline-processor [this selector processor])
  (attach-sink-processor [this selector processor])
  (detach-processor [this processor]))
