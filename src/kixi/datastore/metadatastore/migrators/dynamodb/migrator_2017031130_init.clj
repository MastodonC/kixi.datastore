(ns kixi.datastore.metadatastore.migrators.dynamodb.migrator-2017031130-init
  (:require [kixi.datastore.system :refer [config]]
            [kixi.datastore.application :refer [profile]]
            [kixi.datastore.metadatastore.dynamodb :as mdb]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.cloudwatch :refer [table-dynamo-alarms]]
            [taoensso.faraday :as far]
            [taoensso.timbre :as log]))

(def metadatastore-write-provision 10)
(def metadatastore-read-provision 10)
(def metadatastore-activity-write-provision 10)
(def metadatastore-activity-read-provision 10)

(defn get-db-config
  [db]
  (select-keys db [:endpoint]))

(defn get-alerts-config
  [profile]
  (:alerts (config profile)))

(defn up
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)
        alert-conf (get-alerts-config @profile)]
    (far/create-table conn
                      (mdb/primary-metadata-table profile)
                      [(db/dynamo-col ::md/id) :s]
                      {:throughput {:read metadatastore-read-provision
                                    :write metadatastore-write-provision}
                       :block? true})

    (far/create-table conn
                      (mdb/activity-metadata-table profile)
                      [:groupid-activity :s]
                      {:range-keydef [(db/dynamo-col ::md/id) :s]
                       :throughput {:read metadatastore-activity-read-provision
                                    :write metadatastore-activity-write-provision}
                       :lsindexes [{:name (mdb/activity-metadata-created-index)
                                    :range-keydef [(db/dynamo-col [::md/provenance ::md/created]) :s]
                                    :projection :all}]
                       :block? true})
    (when (:alerts? alert-conf)
      (try
        (table-dynamo-alarms (mdb/primary-metadata-table profile)
                             (assoc alert-conf
                                    :read-provision metadatastore-read-provision
                                    :write-provision metadatastore-write-provision))
        (table-dynamo-alarms (mdb/activity-metadata-table profile)
                             (assoc alert-conf
                                    :read-provision metadatastore-activity-read-provision
                                    :write-provision metadatastore-activity-write-provision))
        (catch Exception e
          (log/error e "Failed to create cloudwatch alarm"))))))

(defn down
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)]
    (far/delete-table conn (mdb/primary-metadata-table profile))
    (far/delete-table conn (mdb/activity-metadata-table profile))))
