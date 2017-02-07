(ns kixi.datastore.filestore.s3
  (:require [amazonica.core :as aws]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3t]
            [byte-streams :as bs]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore]]
            [kixi.datastore.time :as t]
            [taoensso.timbre :as timbre :refer [error]])
  (:import [com.amazonaws.services.s3.model AmazonS3Exception]))

(defn ensure-bucket
  [creds bucket]
  (when-not (s3/does-bucket-exist creds bucket)
    (s3/create-bucket creds bucket)))

(defn create-link
  [creds bucket]
  (fn [id]
    (str
     (s3/generate-presigned-url
      creds
      :bucket-name bucket
      :key id
      :expiration (t/minutes-from-now 30)
      :method "PUT"))))

(defn object-size
  [creds bucket id]
  (try
    (when-let [meta (s3/get-object-metadata creds bucket id)]
      (:instance-length meta))
    (catch AmazonS3Exception e
      nil)))

(def unallowed-chars #"[^\p{Digit}\p{IsAlphabetic}]")
(def multi-hyphens #"-{2,}")

(def hyphen (clojure.string/re-quote-replacement "-"))

(defn clean
  [s]
  (-> s
      (clojure.string/replace unallowed-chars hyphen)
      (clojure.string/replace multi-hyphens hyphen)))

(defn sanitize-filename
  [f-name]
  (let [extension-dex (clojure.string/last-index-of f-name ".")
        extension (subs f-name (inc extension-dex))]
    (-> f-name
        (subs 0 extension-dex)
        clean
        (str "." (clean extension)))))

(defn create-dload-link
  [creds bucket id file-name expiry]
  (let [header-overrides (com.amazonaws.services.s3.model.ResponseHeaderOverrides.)
        _ (when file-name (.setContentDisposition header-overrides (str "attachment; filename=" (sanitize-filename file-name))))]
    (-> (s3/generate-presigned-url creds
                                   :bucket-name bucket 
                                   :key id
                                   :expiration expiry
                                   :method "GET" 
                                   :response-headers header-overrides)
        str)))

(defrecord S3
    [communications logging region endpoint access-key secret-key link-expiration-mins bucket client-options creds]
    FileStore
    (exists [this id]
      (s3/does-object-exist creds bucket id))
    (size [this id]
      (object-size creds bucket id))
    (retrieve [this id]
      (when (s3/does-object-exist creds bucket id)
        (:object-content
         (s3/get-object creds bucket id))))
    (create-link [this id file-name]
      (when (s3/does-object-exist creds bucket id)
        (create-dload-link creds bucket id file-name
                           (t/minutes-from-now link-expiration-mins))))
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
                                      {:link-creator (create-link c bucket)})
          (assoc component
                 :creds
                 c))
        component))
    (stop [component]
      (if creds
        (dissoc component
                :creds)
        component)))
