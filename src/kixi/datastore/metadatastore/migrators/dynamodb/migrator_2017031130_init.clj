(ns kixi.datastore.metadatastore.migrators.dynamodb.migrator-2017031130-init
  (:require [kixi.datastore.system :refer [system-profile]]
            [kixi.datastore.metadatastore.dynamodb :as mdb]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.dynamodb :as db]
            [taoensso.faraday :as far]
            [taoensso.timbre :as log]))

(defn get-db-config
  [db]
  (select-keys db
               [:endpoint]))


(defn up
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)]
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
                       :block? true})))

(defn down
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)]
    (far/delete-table conn (mdb/primary-metadata-table profile))
    (far/delete-table conn (mdb/activity-metadata-table profile))))
