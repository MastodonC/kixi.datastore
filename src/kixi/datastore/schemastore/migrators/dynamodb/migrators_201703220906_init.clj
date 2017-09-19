(ns kixi.datastore.schemastore.migrators.dynamodb.migrators-201703220906-init
  (:require [kixi.datastore
             [application :refer [profile]]
             [dynamodb :as db]
             [schemastore :as ss]
             [system :refer [config]]]
            [kixi.datastore.schemastore.dynamodb :as ssdb]
            [kixi.datastore.cloudwatch :refer [table-dynamo-alarms]]
            [taoensso.faraday :as far]
            [taoensso.timbre :as log]))

(def schemastore-write-provision 10)
(def schemastore-read-provision 10)

(defn get-db-config
  [db]
  (select-keys db
               [:endpoint]))

(defn get-alerts-config
  [profile]
  (:alerts (config profile)))

(defn up
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)
        alert-conf (get-alerts-config @profile)]
    (far/create-table conn
                      (ssdb/primary-schemastore-table profile)
                      [(db/dynamo-col ::ss/id) :s]
                      {:throughput {:read schemastore-read-provision
                                    :write schemastore-write-provision}
                       :block? true})
    (when (:alerts? alert-conf)
      (try
        (table-dynamo-alarms (ssdb/primary-schemastore-table profile)
                             (assoc alert-conf
                                    :read-provision schemastore-read-provision
                                    :write-provision schemastore-write-provision))
        (catch Exception e
          (log/error e "Failed to create cloudwatch alarm"))))))

(defn down
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)]
    (far/delete-table conn (ssdb/primary-schemastore-table profile))))
