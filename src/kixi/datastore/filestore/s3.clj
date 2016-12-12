(ns kixi.datastore.filestore.s3
  (:require [amazonica.core :as aws]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3t]
            [byte-streams :as bs]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore]]
            [kixi.datastore.time :as t]
            [taoensso.timbre :as timbre :refer [error]]))

(defn ensure-bucket
  [creds bucket]
  (when-not (s3/does-bucket-exist creds bucket)
    (s3/create-bucket creds bucket)))

(defn create-link
  [creds bucket]
  (fn [id]
    (s3/generate-presigned-url
     creds
     :bucket-name bucket
     :key id
     :expiration (t/minutes-from-now 30)
     :method "PUT")))

(defrecord S3
    [communications logging region endpoint access-key secret-key bucket client-options creds]
  FileStore
  (exists [this id]
    (s3/does-object-exist creds bucket id))
  (retrieve [this id]
    (when (s3/does-object-exist creds bucket id)
      (:object-content
       (s3/get-object creds bucket id))))
  component/Lifecycle
  (start [component]
    (if-not creds
      (let [c (merge {:endpoint endpoint}
                     (when secret-key
                       {:secret-key secret-key})
                     (when access-key
                       {:access-key access-key}))]
        (ensure-bucket c bucket)
        (fs/attach-command-handlers communications
                                    {:create-upload-link-handler (create-link c bucket)})
        (assoc component
               :creds
               c))
      component))
  (stop [component]
    (if creds
      (dissoc component
              :creds)
      component)))
