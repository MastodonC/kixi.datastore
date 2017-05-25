(ns kixi.datastore.metadatastore.license
  (:require [clojure.spec :as s]))

(s/def ::usage string?)
(s/def ::type string?)

(s/def ::license
  (s/keys :req [::type ::usage]))

