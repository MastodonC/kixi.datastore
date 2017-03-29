(ns kixi.datastore.schemastore.migrators.dynamodb.migrators-201703220906-init
  (:require [kixi.datastore
             [dynamodb :as db]
             [schemastore :as ss]
             [system :refer [system-profile config]]]
            [kixi.datastore.schemastore.dynamodb :as ssdb]
            [kixi.datastore.cloudwatch :refer [table-dynamo-alarms]]
            [taoensso.faraday :as far]))

(defn get-db-config
  [db]
  (select-keys db
               [:endpoint]))

(defn get-alerts-config
  [profile]
  (:alerts (config profile)))

(defn up
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)
        alert-conf (get-alerts-config @system-profile)]
    (far/create-table conn
                      (ssdb/primary-schemastore-table profile)
                      [(db/dynamo-col ::ss/id) :s]
                      {:throughput {:read 10 :write 10}
                       :block? true})
    (when (:alerts? alert-conf)
      (try
        (table-dynamo-alarms (ssdb/primary-schemastore-table profile) alert-conf)
        (catch Exception e
          (log/error e "Failed to create cloudwatch alarm"))))))

(defn down
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)]
    (far/delete-table conn (ssdb/primary-schemastore-table profile))))
