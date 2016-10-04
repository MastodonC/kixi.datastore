(ns kixi.datastore.schemastore
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore.validator]))

(s/def ::name :kixi.datastore.schemastore.validator/valid-schema-name)
(s/def ::definition (constantly true))

(s/def ::create-request
  (s/keys :req [::name ::definition]))

(defprotocol SchemaStore
  (exists [this spec-name])
  (fetch-definition [this spec-name])
  (fetch-spec [this spec-name]))
