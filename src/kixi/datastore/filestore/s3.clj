(ns kixi.datastore.filestore.s3
  (:require [amazonica.core :as aws]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3t]
            [byte-streams :as bs]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore]]))

(defn ensure-bucket
  [creds bucket]
  (when-not (s3/does-bucket-exist creds bucket)
    (s3/create-bucket creds bucket)))

(def ^Integer buffer-size 
  "The piped stream must have a buffer size at least as big as the S3 clients: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/constant-values.html#com.amazonaws.RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE"
  (inc 131073))

(defn upload
  [creds bucket id content-length]
  (let [in-stream (new java.io.PipedInputStream buffer-size)
        out-stream (new java.io.PipedOutputStream in-stream)]
    [(go
       (s3/put-object creds
                      bucket id in-stream 
                      {:content-length content-length}))
     out-stream]))

(defrecord S3
    [logging region endpoint access-key secret-key bucket client-options creds]
    FileStore
    (exists [this id]
      (s3/does-object-exist creds bucket id))
    (output-stream [this id content-length]
      (upload creds bucket id content-length))
    (retrieve [this id]
      (when (s3/does-object-exist creds bucket id)
        (:object-content
         (s3/get-object creds bucket id))))
    component/Lifecycle
    (start [component]      
      (if-not creds
        (let [c (if endpoint 
                  {:endpoint endpoint
                   :secret-key secret-key
                   :access-key access-key}
                  {:region region})]
          (ensure-bucket c bucket)
          (assoc component
                 :creds
                 c))
        component))
    (stop [component]
      (if creds
        (dissoc component
                :creds)
        component)))
