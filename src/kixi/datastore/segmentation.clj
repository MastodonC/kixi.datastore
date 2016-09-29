(ns kixi.datastore.segmentation
  (:require [clojure.spec :as s]))

(s/def ::id string?)
(s/def ::column-name string?)
(s/def ::line-count int?)
(s/def ::value (constantly true))
(s/def ::created boolean?)
(s/def ::msg #{:unknown-file-type})
(s/def ::type #{::group-rows-by-column})

(s/def ::segment-ids (s/cat :ids (s/+ ::id)))

(defprotocol Segmentation)
