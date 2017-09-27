(ns kixi.integration.schema
  {:integration true}
  (:require [clojure.spec.test.alpha :refer [with-instrument-disabled]]
            [clojure.test :refer :all]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]]
            [kixi.integration.base :as base :refer :all]
            [medley.core :refer [dissoc-in]]))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(defn metadata-file-schema
  [uid]
  {::ss/name ::metadata-file-schema
   ::ss/schema {::ss/type "list"
                ::ss/definition ["cola" {::ss/type "integer"}
                                 "colb" {::ss/type "integer"}]}
   ::ss/provenance {::ss/source "upload"
                    :kixi.user/id uid}
   ::ss/sharing {::ss/read [uid]
                 ::ss/use [uid]}})

(def get-schema-id (comp schema->schema-id metadata-file-schema))

(defn remove-system-added-elements
  [m]
  (-> m
      (dissoc-in [::ss/provenance ::ss/created])
      (dissoc ::ss/id)))

(deftest send-and-retrieve-schema
  (let [uid (uuid)
        schema-id (get-schema-id uid)
        resp (get-spec uid schema-id)]
    (when-success resp
      (let [retrieved (extract-schema resp)]
        (is (get-in retrieved [::ss/provenance ::ss/created]))
        (is (get retrieved ::ss/id))
        (is-match (metadata-file-schema uid)
                  (update (remove-system-added-elements retrieved)
                          ::ss/name keyword))))))
