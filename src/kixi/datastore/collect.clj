(ns kixi.datastore.collect
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.collect.commands]
            [kixi.datastore.collect.events]
            [kixi.comms :as comms]))

(s/def ::message sc/not-empty-string)
(s/def ::groups  (s/coll-of :kixi.group/id))
(s/def ::sender :kixi/user)

(defmethod comms/command-type->event-types
  [:kixi.datastore.collect/request-collection "1.0.0"]
  [_]
  #{[:kixi.datastore.collect/collection-requested "1.0.0"]
    [:kixi.datastore.collect/collection-request-rejected "1.0.0"]})

(defprotocol CollectAndShare)
