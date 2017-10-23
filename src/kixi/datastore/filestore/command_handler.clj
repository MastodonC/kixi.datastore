(ns kixi.datastore.filestore.command-handler
  (:require [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'event 'kixi.event)
(sh/alias 'ms 'kixi.datastore.metadatastore)
(sh/alias 'fs 'kixi.datastore.filestore)
(sh/alias 'up 'kixi.datastore.filestore.upload)

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn create-upload-cmd-handler
  [link-fn]
  (fn [e]
    (let [id (uuid)]
      {:kixi.comms.event/key ::fs/upload-link-created
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/partition-key id
       :kixi.comms.event/payload {::fs/upload-link (link-fn id)
                                  ::fs/id id
                                  :kixi.user/id (get-in e [:kixi.comms.command/user :kixi.user/id])}})))

(defn create-multi-part-upload-cmd-handler
  [multi-part-upload-fn]
  (fn [cmd]
    (let [part-count (::up/part-count cmd)
          id (uuid)
          {:keys [upload-part-urls upload-id]} (multi-part-upload-fn id part-count)]
      [{::event/type ::fs/multi-part-upload-links-created
        ::event/version "1.0.0"
        ::up/part-urls upload-part-urls
        ::up/id upload-id
        ::fs/id id}
       {:partition-key id}])))

(defn create-complete-multi-part-upload-cmd-handler
  [complete-multi-part-upload-fn]
  (fn [cmd]
    (let [{:keys [kixi.datastore.filestore.upload/part-ids
                  kixi.datastore.filestore/id]} cmd
          upload-id (:kixi.datastore.filestore.upload/id cmd)]
      (complete-multi-part-upload-fn id part-ids upload-id)
      [{::event/type ::fs/multi-part-upload-completed
        ::event/version "1.0.0"
        ::up/id upload-id
        ::fs/id id}
       {:partition-key id}])))
