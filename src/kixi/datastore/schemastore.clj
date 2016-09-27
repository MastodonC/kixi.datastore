(ns kixi.datastore.schemastore
  (:require [clojure.spec :as s]))

(s/def id string?)

(defprotocol SchemaStore
  (fetch-definition [this spec-name])
  (fetch-spec [this spec-name])
  (persist [this spec-name spec-definition]))
