(ns kixi.datastore.metadatastore.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore
             :refer [MetaDataStore] :as kdms]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [taoensso.timbre :as timbre :refer [error info infof]])
  (:import [kixi.datastore.metadatastore FileMetaData]))

(defn update-metadata-processor
  [data]
  (fn [^FileMetaData metadata]
    (info "Update: " metadata)
    (swap! data 
           #(update % (:id metadata)
                    (fn [current-metadata]
                      (kdms/map->FileMetaData
                       (merge current-metadata
                              metadata)))))))

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
                                 #(instance? FileMetaData %)
                                 (update-metadata-processor new-data))
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Metadata Store")
            (reset! data {})
            (dissoc component :data))
        component)))
