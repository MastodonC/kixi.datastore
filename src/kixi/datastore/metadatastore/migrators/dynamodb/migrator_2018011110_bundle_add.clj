(ns kixi.datastore.metadatastore.migrators.dynamodb.migrator-2018011110-bundle-add
  (:require [kixi.datastore.system :refer [read-config]]
            [kixi.datastore.application :refer [profile config-location]]
            [kixi.datastore.metadatastore.dynamodb :as mdb]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.cloudwatch :refer [table-dynamo-alarms]]
            [taoensso.faraday :as far]
            [taoensso.timbre :as log]))

(defn get-db-config
  [db]
  (select-keys db [:endpoint]))

(defn up
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)]))

(defn down
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)]))
