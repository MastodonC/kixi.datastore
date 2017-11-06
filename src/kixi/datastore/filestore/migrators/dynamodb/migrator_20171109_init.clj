(ns kixi.datastore.filestore.migrators.dynamodb.migrator-20171109-init
  (:require [kixi.datastore.system :refer [read-config]]
            [kixi.datastore.application :refer [profile config-location]]
            [kixi.datastore.filestore.upload-cache.dynamodb :as fsdb]
            [kixi.datastore.filestore :as fs]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.cloudwatch :refer [table-dynamo-alarms]]
            [taoensso.faraday :as far]
            [taoensso.timbre :as log]))

(def filestore-upload-write-provision 10)
(def filestore-upload-read-provision 10)

(defn get-db-config
  [db]
  (select-keys db [:endpoint]))

(defn get-alerts-config
  [profile]
  (:alerts (read-config @config-location profile)))

(defn up
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)
        alert-conf (get-alerts-config profile)]
    (far/create-table conn
                      (fsdb/primary-upload-cache-table profile)
                      [(db/dynamo-col ::fs/id) :s]
                      {:throughput {:read filestore-upload-read-provision
                                    :write filestore-upload-write-provision}
                       :block? true})
    (when (:alerts? alert-conf)
      (try
        (table-dynamo-alarms (fsdb/primary-upload-cache-table profile)
                             (assoc alert-conf
                                    :read filestore-upload-read-provision
                                    :write filestore-upload-write-provision))
        (catch Exception e
          (log/error e "Failed to create cloudwatch alarm"))))))

(defn down
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)]
    (far/delete-table conn (fsdb/primary-upload-cache-table profile))))
