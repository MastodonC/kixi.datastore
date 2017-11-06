(ns kixi.datastore.filestore.upload-cache.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.time :as t]
            [kixi.datastore.filestore :as fs :refer [FileStoreUploadCache]]
            [kixi.datastore.filestore.upload :as up]
            [taoensso.timbre :as log]))

(defrecord InMemory
    [communications profile endpoint
     cache]
  FileStoreUploadCache
  (get-item [this file-id]
    (get @cache file-id))
  (put-item! [this file-id mup? user upload-id]
    (let [m {::fs/id file-id
             ::up/id upload-id
             ::up/mup? mup?
             :kixi/user user
             ::up/started-at (t/timestamp)
             ::up/finished-at nil}]
      (log/info "Adding" m "for upload" file-id)
      (swap! cache assoc file-id m)))
  component/Lifecycle
  (start [component]
    (log/info "Starting InMemory FileStoreUploadCache")
    (assoc component :cache (atom {})))
  (stop [component]
    (log/info "Stopping InMemory FileStoreUploadCache")
    component))
