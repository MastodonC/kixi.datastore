(ns kixi.datastore.metadatastore.geography
  (:require [clojure.spec.alpha :as s]))

(s/def ::level string?)
(s/def ::type #{"smallest"})

(defmulti geography ::type)

(defmethod geography "smallest"
  [_]
  (s/keys :req [::type
                ::level]))

(s/def ::geography
  (s/multi-spec geography ::type))
