(ns kixi.datastore.metadatastore.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore
             :refer [MetaDataStore]]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [taoensso.timbre :as timbre :refer [error info infof]])
  (:import [kixi.datastore.metadatastore DocumentMetaData]))

(defn update-metadata-processor
  [data]
  (fn [^DocumentMetaData metadata]
    (info "Update: " metadata)
    (swap! data 
           #(update % (:id metadata)
                    (fn [current-metadata]
                      (merge current-metadata
                             metadata))))))

(defrecord InMemory
    [data communications]
    MetaDataStore
    (fetch [this id]
      (get @data id))
    (query [this criteria])

    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Metadata Store")
          (attach-sink-processor communications
                                 (partial instance? DocumentMetaData)
                                 (update-metadata-processor new-data))
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Metadata Store")
            (reset! data {})
            (dissoc component :data))
        component)))
