(ns kixi.datastore.filestore.event-handler
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [medley.core :as m]
            [taoensso.timbre :as log]
            [kixi.datastore.schemastore.utils :as sh]
            [kixi.datastore.filestore :as fs :refer [FileStoreUploadCache]]))

(sh/alias 'up 'kixi.datastore.filestore.upload)

(defn create-file-upload-initiated-event-handler
  [cache]
  (fn [{:keys [::up/part-urls ::fs/id kixi/user kixi.event/created-at] :as event}]
    (let [upload-id (::up/id event)
          mup?  (> (count part-urls) 1)]
      (fs/put-item! cache id mup? user upload-id created-at)
      nil)))

(defn create-file-upload-completed-event-handler
  [cache]
  (fn [{:keys [::fs/id] :as event}]
    (fs/delete-item! cache id)
    nil))

(defn create-file-upload-failed-or-rejected-event-handler
  [cache]
  (fn [{:keys [::fs/id] :as event}]
    (when id
      (fs/delete-item! cache id))
    nil))
