(ns kixi.datastore.metadatastore.time
  (:require  [clojure.spec :as s]
             [kixi.datastore.schemastore.conformers :as sc]))

(s/def ::from (s/nilable sc/timestamp))
(s/def ::to (s/nilable sc/timestamp))

(s/def ::temporal-coverage
  (s/keys :opt [::from ::to]))