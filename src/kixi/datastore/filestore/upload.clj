(ns kixi.datastore.filestore.upload
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.conformers :as sc]))

(s/def ::id string?)
(s/def ::part-count int?)
(s/def ::part-ids (s/coll-of sc/not-empty-string))
(s/def ::part-urls (s/coll-of sc/not-empty-string))
