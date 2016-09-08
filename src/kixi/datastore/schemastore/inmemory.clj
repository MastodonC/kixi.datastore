(ns kixi.datastore.schemastore.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [kixi.datastore.schemastore :refer [SchemaStore]]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [taoensso.timbre :as timbre :refer [error info infof]])
  (:import [kixi.datastore.schemastore Schema]))

(defn update-schema-processor
  [data]
  (fn [^Schema schema]
    (info "Update: " schema)
    (swap! data 
           #(update % (:id schema)
                    (fn [current-schema]
                      (merge current-schema
                             schema))))))

(defrecord InMemory
    [data communications]
    SchemaStore
    (fetch [id]
      (get @data id))
    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Schema Store")
          (attach-sink-processor communications
                                 (partial instance? Schema)
                                 (update-schema-processor new-data))
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Schema Store")
            (reset! data {})
            (dissoc component :data))
        component)))
