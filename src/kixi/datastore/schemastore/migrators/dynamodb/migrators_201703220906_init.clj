(ns kixi.datastore.schemastore.migrators.dynamodb.migrators-201703220906-init
  (:require [kixi.datastore
             [dynamodb :as db]
             [schemastore :as ss]
             [system :refer [system-profile]]]
            [kixi.datastore.schemastore.dynamodb :as ssdb]
            [taoensso.faraday :as far]))

(defn get-db-config
  [db]
  (select-keys db
               [:endpoint]))

(defn up
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)]
    (far/create-table conn
                      (ssdb/primary-schemastore-table profile)
                      [(db/dynamo-col ::ss/id) :s]
                      {:throughput {:read 10 :write 10}
                       :block? true})))

(defn down
  [db]
  (let [profile (name @system-profile)
        conn (get-db-config db)]
    (far/delete-table conn (ssdb/primary-schemastore-table profile))))

