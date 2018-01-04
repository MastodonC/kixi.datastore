(ns kixi.datastore.filestore.upload-cache.dynamodb
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]
            [kixi.datastore.time :as t]
            [kixi.datastore.filestore.upload :as up]
            [kixi.datastore.filestore :as fs :refer [FileStoreUploadCache]]
            [kixi.datastore.dynamodb :as db :refer [migrate]]))

(def id-col (db/dynamo-col ::fs/id))

(defn primary-upload-cache-table
  [profile]
  (str profile "-kixi.datastore-filestore-upload-cache"))

(defrecord DynamoDb
    [communications profile
     client get-item-fn put-item-fn delete-item-fn]
  FileStoreUploadCache
  (get-item [this file-id]
    (log/debug "Getting upload item from Dynamo" file-id)
    (not-empty (get-item-fn file-id)))
  (put-item! [this file-id mup? user upload-id created-at]
    (log/debug "Putting upload item into Dynamo" file-id mup?)
    (put-item-fn {::fs/id file-id
                  ::up/id upload-id
                  ::up/mup? mup?
                  ::up/ttl (t/since-epoch (t/add-hours (t/from-str created-at) 72))
                  :kixi/user user
                  ::up/created-at created-at}))
  (delete-item! [this file-id]
    (log/debug "Deleting upload item from Dynamo" file-id)
    (delete-item-fn {::fs/id file-id}))
  component/Lifecycle
  (start [component]
    (if-not client
      (let [client (assoc (select-keys (:dynamodb component)
                                       db/client-kws)
                          :profile profile)
            joplin-conf {:migrators {:migrator "joplin/kixi/datastore/filestore/migrators/dynamodb"}
                         :databases {:dynamodb (merge
                                                {:type :dynamo
                                                 :migration-table (str profile "-kixi.datastore-filestore.upload.migrations")}
                                                client)}
                         :environments {:env [{:db :dynamodb :migrator :migrator}]}}]
        (log/info "Starting DynamoDb FileStoreUploadCache -" profile)
        (migrate :env joplin-conf)
        (assoc component
               :client client
               :put-item-fn (partial db/put-item client (primary-upload-cache-table profile))
               :get-item-fn (partial db/get-item client (primary-upload-cache-table profile) id-col)
               :delete-item-fn (partial db/delete-item client (primary-upload-cache-table profile))))
      component))
  (stop [component]
    (if client
      (do
        (log/info "Stopping DynamoDb FileStoreUploadCache")
        (dissoc component :client :get-item-fn :put-item-fn :delete-item-fn))
      component)))
