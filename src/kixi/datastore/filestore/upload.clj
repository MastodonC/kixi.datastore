(ns kixi.datastore.filestore.upload
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.conformers :as sc]))

(s/def ::part-count int?)
(s/def ::part-ids (s/coll-of sc/not-empty-string))

(s/def ::start-byte int?)
(s/def ::length-bytes int?)
(s/def ::part-url (s/keys :req [::start-byte
                                ::length-bytes
                                ::url]))
(s/def ::part-urls (s/coll-of ::part-url))
(s/def ::size-bytes int?)

;; db
(s/def ::id string?)
(s/def ::mup? boolean?)
(s/def ::started-at sc/timestamp)
(s/def ::finished-at sc/timestamp)
