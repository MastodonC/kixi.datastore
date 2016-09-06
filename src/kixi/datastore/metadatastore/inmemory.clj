(ns kixi.datastore.metadatastore.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore.metadatastore
             :refer [MetaDataStore]]
            [kixi.datastore.communications.communications
             :refer [attach-processor 
                     metadata-new-selector 
                     metadata-update-selector]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn new-metadata-processor
  [data] 
  (fn [metadata]
    (info "New: " metadata)
    (swap! data 
           #(update % (:id metadata)
                    (fn [current-metadata]
                      (merge current-metadata
                             metadata))))))

(defn update-metadata-processor
  [data]
  (fn [metadata]
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
      (when-not data
        (info "Starting InMemory Metadata Store")
        (let [new-data (atom {})]
          (attach-processor communications
                            metadata-new-selector
                            (new-metadata-processor new-data))
          (attach-processor communications 
                            metadata-update-selector
                            (update-metadata-processor new-data))
          (assoc component :data new-data))))
    (stop [component]
      (info "Destroying InMemory Metadata Store")
      (when data
        (reset! data {})
        (dissoc component :data))))
