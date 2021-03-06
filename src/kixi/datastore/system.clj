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
             [repl :as repl]]
            [kixi.datastore.filestore
             [local :as local]
             [s3 :as s3]]
            [kixi.datastore.filestore.upload-cache
             [inmemory :as fscache-inmemory]
             [dynamodb :as fscache-dd]]
            [kixi.comms :as comms]
            [kixi.comms.components
             [kinesis :as kinesis]
             [coreasync :as coreasync]]
            [kixi.datastore.metadatastore
             [inmemory :as md-inmemory]
             [dynamodb :as md-dd]]
            [kixi.datastore.metadatastore.command-handler
             :as md-creator]
            [kixi.datastore.schemastore
             [inmemory :as ss-inmemory]
             [dynamodb :as ss-dd]]
            [kixi.datastore.segmentation
             [inmemory :as segementation-inmemory]]
            [taoensso.timbre :as log]
            [kixi.log :as kixi-log]
            [medley.core :as med]))

(defmethod aero/reader 'rand-uuid
 [{:keys [profile] :as opts} tag value]
  (str (java.util.UUID/randomUUID)))

(defn read-config
  "Read EDN config, with the given profile. See Aero docs at
  https://github.com/juxt/aero for details."
  [config-location profile]
  (aero/read-config (io/resource config-location) {:profile profile}))

(def component-dependencies
  {:metrics []
   :repl []
   :logging [:metrics]
   :communications []
   :web-server [:metrics :logging :filestore :metadatastore :schemastore :communications]
   :filestore-upload-cache []
   :filestore [:logging :communications :filestore-upload-cache]
   :metadatastore [:communications]
   :metadata-creator [:communications :filestore :schemastore :metadatastore]
   :schemastore [:communications]
                                        ;   :schema-extracter [:communications :filestore]
   :segmentation [:communications :metadatastore :filestore]
   :structural-validator [:communications :filestore :schemastore]})

(defn new-system-map
  [config]
  (apply system-map
         ((comp flatten seq merge)
          {:web-server (web-server/map->WebServer {})
           :metrics (metrics/map->Metrics {})
           :repl (repl/map->ReplServer {})
           :logging (logging/map->Log {})
           :metadatastore (case (first (keys (:metadatastore config)))
                            :inmemory (md-inmemory/map->InMemory {})
                            :dynamodb (md-dd/map->DynamoDb {}))
           :communications (case (first (keys (:communications config)))
                             :kinesis (kinesis/map->Kinesis {})
                             :coreasync (coreasync/map->CoreAsync {}))}
          (when (:filestore-upload-cache config)
            {:filestore-upload-cache
             (case (first (keys (:filestore-upload-cache config)))
               :inmemory (fscache-inmemory/map->InMemory {})
               :dynamodb (fscache-dd/map->DynamoDb {}))})
          (when (:filestore config)
            {:filestore
             (case (first (keys (:filestore config)))
               :local (local/map->Local {})
               :s3 (s3/map->S3 {}))})
          (when (:schemastore config)
            {:schemastore
             (case (first (keys (:schemastore config)))
               :inmemory (ss-inmemory/map->InMemory {})
               :dynamodb (ss-dd/map->DynamoDb {}))})
          (when (get-in config [:metadata-creator :enabled])
            {:metadata-creator (md-creator/map->MetadataCreator {})})
          (when (get-in config [:structural-validator :enabled])
            {:structural-validator (sv/map->StructuralValidator {})})
          (when (:segmentation config)
            {:segmentation (case (first (keys (:segmentation config)))
                             :inmemory (segementation-inmemory/map->InMemory {}))}))))

(defn raise-first
  "Updates the keys value in map to that keys current first value"
  [m k]
  (if-let [v (first (vals (k m)))]
          (assoc m k v)
          m))

(defn configure-components
  "Merge configuration to its corresponding component (prior to the
  system starting). This is a pattern described in
  https://juxt.pro/blog/posts/aero.html"
  [system config profile]
  (->> (-> config
           (raise-first :filestore)
           (raise-first :metadatastore)
           (raise-first :communications)
           (raise-first :schemastore)
           (raise-first :segmentation))
       (med/map-vals #(if (map? %)
                        (assoc % :profile (name profile))
                        %))
       (merge-with merge
                   system)))

(defn configure-logging
  [config]
  (let [level-config {:level (get-in config [:logging :level])
                      :ns-blacklist (get-in config [:logging :ns-blacklist])
                      :timestamp-opts kixi-log/default-timestamp-opts ; iso8601 timestamps
                      :appenders (case (get-in config [:logging :appender])
                                   :println {:println (log/println-appender)}
                                   :json {:direct-json (kixi-log/timbre-appender-logstash)})}]
    (log/set-config! level-config)
    (log/handle-uncaught-jvm-exceptions!
     (fn [throwable ^Thread thread]
       (log/error throwable (str "Unhandled exception on " (.getName thread)))))
    (when (get-in config [:logging :kixi-comms-verbose-logging])
      (log/info "Switching on Kixi Comms verbose logging...")
      (comms/set-verbose-logging! true))))

(defn system-using-available
  [system-map dependencies]
  (system-using system-map
                (med/map-vals
                 #(filterv (set (keys system-map)) %)
                 (select-keys dependencies
                              (keys system-map)))))

(defn new-system
  [config-location profile]
  (let [config (read-config config-location profile)]
    (configure-logging config)
    (-> (new-system-map config)
        (configure-components config profile)
        (system-using-available component-dependencies))))
