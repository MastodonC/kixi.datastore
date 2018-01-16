(ns kixi.datastore.collect.event-handler
  (:require [kixi.datastore.collect.events]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'c 'kixi.datastore.collect)

(defn create-collection-requested-handler
  [metadatastore]
  (fn [event]))
