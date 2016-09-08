(ns kixi.datastore.schemastore.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [kixi.datastore.schemastore :refer [SchemaStore]]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defrecord InMemory
    [data write-schema communications]
    SchemaStore
    (fetch [id]
      (get @data id))
    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Schema Store")
          #_(attach-sink-processor communications
                                 (constantly true)
                                 (update-metadata-processor new-data))
          (assoc component :data new-data)))      component)
    (stop [component]
      component))
