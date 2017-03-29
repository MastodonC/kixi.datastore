(ns kixi.datastore.metadatastore.migrators.dynamodb.migrator-2017031130-init
  (:require [kixi.datastore.system :refer [system-profile config]]
            [kixi.datastore.metadatastore.dynamodb :as mdb]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.cloudwatch :refer [table-dynamo-alarms]]
            [taoensso.faraday :as far]
            [taoensso.timbre :as log]))

(defn get-db-config
  [db]
  (select-keys db [:endpoint]))

(defn get-alerts-config
  [profile]
  (:alerts (config profile)))

(defn up
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)
        alert-conf (get-alerts-config @system-profile)]
    (far/create-table conn
                      (mdb/primary-metadata-table profile)
                      [(db/dynamo-col ::md/id) :s]
                      {:throughput {:read 10 :write 10}
                       :block? true})

    (far/create-table conn
                      (mdb/activity-metadata-table profile)
                      [:groupid-activity :s]
                      {:range-keydef [(db/dynamo-col ::md/id) :s]
                       :throughput {:read 10 :write 10}
                       :lsindexes [{:name (mdb/activity-metadata-created-index)
                                    :range-keydef [(db/dynamo-col [::md/provenance ::md/created]) :s]
                                    :projection :all}]
                       :block? true})
    (when (:alerts? alert-conf)
      (try
        (table-dynamo-alarms (mdb/primary-metadata-table profile) alert-conf)
        (table-dynamo-alarms (mdb/activity-metadata-table profile) alert-conf)
        (catch Exception e
          (log/error e "Failed to create cloudwatch alarm"))))))

(defn down
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)]
    (far/delete-table conn (mdb/primary-metadata-table profile))
    (far/delete-table conn (mdb/activity-metadata-table profile))))
