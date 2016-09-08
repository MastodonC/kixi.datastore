(ns kixi.datastore.schemastore.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [kixi.datastore.schemastore :refer [SchemaStore]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defrecord InMemory
    []
    SchemaStore
    (fetch [id])
    component/Lifecycle
    (start [component]
      component)
    (stop [component]
      component))
