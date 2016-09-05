(ns kixi.datastore.schemaextracter
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.protocols 
             :as p
             :refer [update-metadata
                     attach-new-metadata-processor
                     attach-update-metadata-processor]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn download
  [])

(defn new-metadata-processor
  [communications]
  (fn [metadata]
    ))

(defn update-metadata-processor
  [communications]
  (fn [metadata]))

(defrecord SchemaExtracter
    [communications]
    p/SchemaExtracter
    component/Lifecycle
    (start [component]
      (info "Starting SchemaExtracter")
      (attach-new-metadata-processor communications
                                     (new-metadata-processor communications))
      (attach-update-metadata-processor communications
                                        (update-metadata-processor communications)))
    (stop [component]
      (info "Stopping SchemaExtracter")
      ;Need a 'detach' processor ability
      ))
