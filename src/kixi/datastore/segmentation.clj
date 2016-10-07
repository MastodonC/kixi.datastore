(ns kixi.datastore.segmentation
  (:require [clojure.spec :as s]))

(defn sha1?
  [x]
  (re-find #"^[0-9a-f]{40}$" x))

(s/def ::id string?)
(s/def ::sha1 sha1?)
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
