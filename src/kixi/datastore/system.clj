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
             [schemaextracter :as se]
             [structural-validation :as sv]
             [sharing :as share]]            
            [kixi.datastore.filestore
             [local :as local]
             [s3 :as s3]]
            [kixi.datastore.communications
             [coreasync :as coreasync]]
            [kixi.comms.components
             [kafka :as kafka]]
            [kixi.datastore.metadatastore
             [inmemory :as md-inmemory]]
            [kixi.datastore.schemastore
             [inmemory :as ss-inmemory]]
            [kixi.datastore.segmentation
             [inmemory :as segementation-inmemory]]
            [taoensso.timbre :as log]
            [kixi.datastore.sharing :as share]))

(defmethod aero/reader 'rand-uuid
 [{:keys [profile] :as opts} tag value]
  (str (java.util.UUID/randomUUID)))

(defn config
  "Read EDN config, with the given profile. See Aero docs at
  https://github.com/juxt/aero for details."
  [profile]
  (aero/read-config (io/resource "config.edn") {:profile profile}))

(def component-dependencies
  {:metrics [] 
   :logging [:metrics]
   :communications []
   :sharing [:metadatastore]
   :web-server [:metrics :logging :filestore :metadatastore :schemastore :communications :sharing]
   :filestore []
   :metadatastore [:communications]
   :schemastore [:communications]
                                        ;   :schema-extracter [:communications :filestore]
   :segmentation [:communications :metadatastore :filestore]
   :structural-validator [:communications :filestore :schemastore]})

(defn new-system-map
  [config]
  (system-map
   :web-server (web-server/map->WebServer {})
   :metrics (metrics/map->Metrics {})
   :logging (logging/map->Log {})
   :sharing (share/map->Sharing {})
   :filestore (case (first (keys (:filestore config)))
                    :local (local/map->Local {})
                    :s3 (s3/map->S3 {}))
   :metadatastore (case (first (keys (:metadatastore config)))
                    :inmemory (md-inmemory/map->InMemory {}))
   :schemastore (case (first (keys (:schemastore config)))
                    :inmemory (ss-inmemory/map->InMemory {}))
   :segmentation (case (first (keys (:schemastore config)))
                    :inmemory (segementation-inmemory/map->InMemory {}))
   :communications (case (first (keys (:communications config)))
                     :kafka (kafka/map->Kafka {}))
 ;  :schema-extracter (se/map->SchemaExtracter {})
   :structural-validator (sv/map->StructuralValidator {})))

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
                  (raise-first :filestore)
                  (raise-first :metadatastore)
                  (raise-first :communications)
                  (raise-first :schemastore)
                  (raise-first :segmentation))))

(defn configure-logging
  [config]
  (let [level-config {:level (get-in config [:logging :level])
                      :ns-blacklist (get-in config [:logging :ns-blacklist])}]
    (log/merge-config! level-config)
    (log/handle-uncaught-jvm-exceptions! 
     (fn [throwable ^Thread thread]
       (log/error throwable (str "Unhandled exception on " (.getName thread)))))))

(defn new-system
  [profile]
  (let [config (config profile)]
    (configure-logging config)
    (-> (new-system-map config)
        (configure-components config)
        (system-using component-dependencies))))
