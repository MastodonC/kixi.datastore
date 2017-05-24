(ns kixi.datastore.metadatastore.time
  (:require  [clojure.spec :as s]
             [kixi.datastore.schemastore.conformers :as sc]))

(s/def ::from sc/timestamp)
(s/def ::to sc/timestamp)

(s/def ::temporal-coverage
  (s/keys :req [::from ::to]))
