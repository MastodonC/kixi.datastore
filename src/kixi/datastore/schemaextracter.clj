(ns kixi.datastore.schemaextracter
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.communications.communications
             :refer [update-metadata
                     attach-processor
                     metadata-new-selector
                     metadata-update-selector]]
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

(defprotocol ISchemaExtracter)

(defrecord SchemaExtracter
    [communications]
    ISchemaExtracter
    component/Lifecycle
    (start [component]
      (info "Starting SchemaExtracter")
      (attach-processor communications
                        metadata-new-selector
                        (new-metadata-processor communications))
      (attach-processor communications
                        metadata-update-selector
                        (update-metadata-processor communications)))
    (stop [component]
      (info "Stopping SchemaExtracter")
                                        ;Need a 'detach' processor ability
      ))
