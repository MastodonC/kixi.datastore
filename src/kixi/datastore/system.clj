(ns kixi.datastore.system
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [com.stuartsierra.component
             :as
             component
             :refer
             [system-map system-using]]
            [kixi.datastore
             [logging :as logging]
             [metrics :as metrics]
             [web-server :as web-server]
             [schemaextracter :as se]]
            [kixi.datastore.documentstore
             [local :as local]
             [s3 :as s3]]
            [kixi.datastore.communications
             [coreasync :as coreasync]]
            [kixi.datastore.metadatastore
             [inmemory :as inmemory]]))

(defmethod aero/reader 'rand-uuid
 [{:keys [profile] :as opts} tag value]
  (str (java.util.UUID/randomUUID)))

(defn config
  "Read EDN config, with the given profile. See Aero docs at
  https://github.com/juxt/aero for details."
  [profile]
  (aero/read-config (io/resource "config.edn") {:profile profile}))

(defn component-dependencies
  []
  {:metrics [] 
   :communications []
   :logging [:metrics]
   :web-server [:metrics :logging :documentstore :metadatastore :communications]
   :documentstore []
   :metadatastore [:communications]
   :schemaextracter [:communications]})

(defn new-system-map
  [config]
  (system-map
   :web-server (web-server/map->WebServer {})
   :metrics (metrics/map->Metrics {})
   :logging (logging/map->Log {})
   :documentstore (case (first (keys (:documentstore config)))
                    :local (local/map->Local {})
                    :s3 (s3/map->S3 {}))
   :metadatastore (case (first (keys (:metadatastore config)))
                    :inmemory (inmemory/map->InMemory {}))
   :communications (case (first (keys (:communications config)))
                     :coreasync (coreasync/map->CoreAsync {}))
   :schemaextracter (se/map->SchemaExtracter {})))

(defn raise-first
  "Updates the keys value in map to that keys current first value"
  [m k]
  (assoc m k
         (first (vals (k m)))))

(defn configure-components
  "Merge configuration to its corresponding component (prior to the
  system starting). This is a pattern described in
  https://juxt.pro/blog/posts/aero.html"
  [system config]
  (merge-with merge
              system
              (-> config
                  (raise-first :documentstore)
                  (raise-first :metadatastore)
                  (raise-first :communications))))

(defn new-system
  [profile]
  (let [config (config profile)]
    (-> (new-system-map config)
        (configure-components config)
        (system-using (component-dependencies)))))
