(ns kixi.datastore.filestore.migrators.dynamodb.migrator-20171221-ttl
  (:require [kixi.datastore.system :refer [read-config]]
            [kixi.datastore.application :refer [profile config-location]]
            [kixi.datastore.filestore.upload-cache.dynamodb :as fsdb]
            [kixi.datastore.filestore.upload :as fsu]
            [kixi.datastore.filestore :as fs]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.cloudwatch :refer [table-dynamo-alarms]]
            [kixi.datastore.time :as t]
            [taoensso.faraday :as far]
            [taoensso.timbre :as log]))

(def ttl-col (db/dynamo-col ::fsu/ttl))

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
        items (not-empty (db/slow-scan conn (fsdb/primary-upload-cache-table profile) (keyword fsdb/id-col)))]
    ;; Set ttl column
    (when-not (or (= "local" profile)
                  (= "local-kinesis" profile))
      (far/update-ttl conn (fsdb/primary-upload-cache-table profile) true ttl-col))
    ;; Then add ttl column
    (when items
      (log/info "Adding" ::fsu/ttl "column to" (count items) "rows")
      (run! #(db/merge-data conn
                            (fsdb/primary-upload-cache-table profile)
                            fsdb/id-col
                            (get % (keyword fsdb/id-col))
                            {::fsu/ttl (t/since-epoch (t/three-days-from-now))}) items))))
(defn down
  [db]
  (let [profile (name @profile)
        conn (get-db-config db)
        items (not-empty (far/scan conn (fsdb/primary-upload-cache-table profile)))]
    ;; Set ttl column
    (when-not (or (= "local" profile)
                  (= "local-kinesis" profile))
      (far/update-ttl conn (fsdb/primary-upload-cache-table profile) false ttl-col))
    ;; Then add ttl column
    (when items
      (log/info "Removing" ::fsu/ttl "column from" (count items) "rows")
      (run! #(db/merge-data conn
                            (fsdb/primary-upload-cache-table profile)
                            fsdb/id-col
                            (:kixi.datastore.filestore_id %)
                            {::fsu/ttl nil}) items))))
