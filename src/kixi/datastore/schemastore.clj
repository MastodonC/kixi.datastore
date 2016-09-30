(ns kixi.datastore.schemastore
  (:require [clojure.spec :as s]))

(s/def ::name string?)
(s/def ::definition (constantly true))

(s/def ::create-request
  (s/keys :req [::name ::definition]))

(defprotocol SchemaStore
  (fetch-definition [this spec-name])
  (fetch-spec [this spec-name]))
