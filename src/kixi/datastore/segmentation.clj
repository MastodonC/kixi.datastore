(ns kixi.datastore.segmentation
  (:require [clojure.spec :as s]))

(s/def ::id string?)
(s/def ::column-name string?)
(s/def ::line-count int?)
(s/def ::value (constantly true))
(s/def ::created boolean?)
(s/def ::reason #{:unknown-file-type :file-not-found :invalid-column})
(s/def ::cause (constantly true))
(s/def ::type #{::group-rows-by-column})

(s/def ::error
  (s/keys :req [::reason ::cause]))

(s/def ::segment-ids (s/cat :ids (s/+ ::id)))

(defprotocol Segmentation)
