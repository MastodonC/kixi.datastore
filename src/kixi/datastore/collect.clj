(ns kixi.datastore.collect
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.collect.commands]
            [kixi.datastore.collect.events]
            [kixi.datastore.collect.command-handler :as ch]
            [kixi.datastore.collect.event-handler :as eh]
            [kixi.comms :as comms]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]))

(s/def ::message sc/not-empty-string)
(s/def ::groups  (s/coll-of :kixi.group/id))
(s/def ::sender :kixi/user)

(defmethod comms/command-type->event-types
  [:kixi.datastore.collect/request-collection "1.0.0"]
  [_]
  #{[:kixi.datastore.collect/collection-requested "1.0.0"]
    [:kixi.datastore.collect/collection-request-rejected "1.0.0"]})

(defrecord CollectAndShare [metadatastore communications]
  component/Lifecycle
  (start [component]
    (log/info "Starting Collect + Share")
    ;;
    (comms/attach-validating-command-handler!
     communications
     :kixi.datastore.collect/request-collection-handler
     :kixi.datastore.collect/request-collection
     "1.0.0"
     (ch/create-request-collection-handler metadatastore))
    ;;
    (comms/attach-validating-event-handler!
     communications
     :kixi.datastore.collect/collection-requested-handler
     :kixi.datastore.collect/collection-requested
     "1.0.0"
     (eh/create-collection-requested-handler metadatastore))
    component)
  (stop [component]
    (log/info "Stopping Collect + Share")
    component))
