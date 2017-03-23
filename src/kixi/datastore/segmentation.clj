(ns kixi.datastore.segmentation
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore.conformers :refer [uuid anything]]))

(s/def ::id uuid)
(s/def ::column-name string?)
(s/def ::line-count int?)
(s/def ::value (s/or :num number?
                     :str string?))
(s/def ::created boolean?)
(s/def ::reason #{:unknown-file-type :file-not-found :invalid-column})
(s/def ::cause string?)
(s/def ::type #{::group-rows-by-column})

(s/def ::error
  (s/keys :req [::reason ::cause]))

(s/def ::segment-ids (s/cat :ids (s/+ ::id)))

(defprotocol Segmentation)
