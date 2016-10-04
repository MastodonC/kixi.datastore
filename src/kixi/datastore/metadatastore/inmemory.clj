(ns kixi.datastore.metadatastore.inmemory
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore
             :refer [MetaDataStore] :as ms]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn update-metadata-processor
  [data]
  (fn [metadata]
    (info "Update: " metadata)
    (swap! data
           #(update % (::ms/id metadata)
                    (fn [current-metadata]
                      (merge current-metadata
                             metadata))))))

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
          (attach-sink-processor communications
                                 #(s/valid? ::ms/filemetadata %)
                                 (update-metadata-processor new-data))
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Metadata Store")
            (reset! data {})
            (dissoc component :data))
        component)))
