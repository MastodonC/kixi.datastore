(ns kixi.datastore.metadatastore.inmemory
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore
             :refer [MetaDataStore] :as ms]
            [kixi.comms :as c]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn update-metadata-processor
  [data]
  (fn [metadata]
    (info "Update: " metadata)
    (swap! data
           #(update % (::ms/id metadata)
                    (fn [current-metadata]
                      (merge current-metadata
                             metadata))))
    metadata))

(defn response-event
  [metadata]
  {:kixi.comms.event/key :kixi.datastore/file-metadata-persisted
   :kixi.comms.event/version "1.0.0"
   :kixi.comms.event/payload metadata})

(defn response-update-event
  [metadata]
  nil)

(defrecord InMemory
    [data communications]
    MetaDataStore
    (exists [this id]
      ((set
        (keys @data))
       id))
    (fetch [this id]
      (get @data id))
    (query [this criteria])

    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Metadata Store")
          (c/attach-event-handler! communications
                                   :metadatastore
                                   :kixi.datastore/file-created
                                   "1.0.0"
                                   (comp response-event (update-metadata-processor new-data) :kixi.comms.event/payload))
          (c/attach-event-handler! communications
                                   :metadatastore-updates
                                   :kixi.datastore/file-metadata-updated
                                   "1.0.0"
                                   (comp response-update-event (update-metadata-processor new-data) :kixi.comms.event/payload))
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Metadata Store")
            (reset! data {})
            (dissoc component :data))
        component)))
