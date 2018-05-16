(ns kixi.datastore.filestore.s3
  (:require [amazonica.core :as aws]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3t]
            [byte-streams :as bs]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore FileStoreUploadCache]]
            [kixi.datastore.filestore.command-handler :as ch]
            [kixi.datastore.filestore.event-handler :as eh]
            [kixi.datastore.filestore.upload :as up]
            [kixi.datastore.time :as t]
            [kixi.comms :as c]
            [taoensso.timbre :as log :refer [error]])
  (:import [com.amazonaws.services.s3.model AmazonS3Exception GeneratePresignedUrlRequest PartETag CompleteMultipartUploadRequest]))

(defn ensure-bucket
  [creds bucket]
  (when-not (s3/does-bucket-exist creds bucket)
    (let [rules [{:max-age-seconds 3000 :allowed-origins ["*"] :allowed-methods [:PUT] :exposed-headers ["etag"] :allowed-headers ["*"]}
                 {:max-age-seconds 3000 :allowed-origins ["*"] :allowed-methods [:GET] :allowed-headers ["*"]}]]
      (s3/create-bucket creds bucket)
      (s3/set-bucket-cross-origin-configuration creds bucket {:rules rules}))))

(defn init-multi-part-upload-creator
  [creds bucket]
  (fn [id part-ranges]
    (let [{:keys [upload-id] :as initate-resp}
          (s3/initiate-multipart-upload creds :bucket-name bucket :key id)
          client (com.amazonaws.services.s3.AmazonS3ClientBuilder/defaultClient)
          links (vec (map-indexed
                      (fn [i p]
                        (assoc p :url
                               (let [^org.joda.time.DateTime exp (t/minutes-from-now (* 60 24)) ;; 24hrs
                                     req (doto (GeneratePresignedUrlRequest. bucket id com.amazonaws.HttpMethod/PUT)
                                           (.setExpiration (.toDate exp))
                                           (.addRequestParameter "partNumber" (str (inc i)))
                                           (.addRequestParameter "uploadId" upload-id))]
                                 (str (.generatePresignedUrl client req))))) part-ranges))]
      {:upload-id upload-id
       :upload-parts links})))

(defn complete-multi-part-upload-creator
  [creds bucket]
  (fn [id etags upload]
    (try (let [upload-id (::up/id upload)
               req (CompleteMultipartUploadRequest. bucket id upload-id (map-indexed #(PartETag. (inc %1) %2) etags))
               client (com.amazonaws.services.s3.AmazonS3ClientBuilder/defaultClient)]
           (.completeMultipartUpload client req)
           [true nil nil])
         (catch com.amazonaws.services.s3.model.AmazonS3Exception e
           (cond
             (clojure.string/starts-with? (.getMessage e) "Your proposed upload is smaller than the minimum allowed size")
             [false :data-too-small (.getMessage e)]
             (clojure.string/starts-with? (.getMessage e) "One or more of the specified parts could not be found")
             [false :file-missing (.getMessage e)]
             :else
             (throw e))))))

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
  (let [header-overrides (com.amazonaws.services.s3.model.ResponseHeaderOverrides.)]
    (when file-name
      (.setContentDisposition header-overrides (str "attachment; filename=" (sanitize-filename file-name))))
    (-> (s3/generate-presigned-url creds
                                   :bucket-name bucket
                                   :key id
                                   :expiration expiry
                                   :method "GET"
                                   :response-headers header-overrides)
        str)))

(defn complete-small-file-upload-creator
  [creds bucket]
  (fn [id part-ids _]
    (if (s3/does-object-exist creds bucket id)
      [true nil nil]
      [false :file-missing nil])))

(defrecord S3
    [communications filestore-upload-cache logging region endpoint access-key secret-key link-expiration-mins bucket client-options
     creds]
  FileStore
  (exists [this id]
    (s3/does-object-exist creds bucket id))
  (size [this id]
    (object-size creds bucket id))
  (retrieve [this id]
    (when (s3/does-object-exist creds bucket id)
      (:object-content (s3/get-object creds bucket id))))
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
        (log/info "Starting S3 FileStore - bucket:" bucket)
        (ensure-bucket c bucket)
        ;; LEGACY
        (c/attach-command-handler!
         communications
         :kixi.datastore/filestore
         :kixi.datastore.filestore/create-upload-link
         "1.0.0" (ch/create-upload-cmd-handler (create-link c bucket)))
        ;; NEW
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-multi-part
         :kixi.datastore.filestore/initiate-file-upload
         "1.0.0" (ch/create-initiate-file-upload-cmd-handler
                  (create-link c bucket)
                  (init-multi-part-upload-creator c bucket)
                  filestore-upload-cache))
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-multi-part-completed
         :kixi.datastore.filestore/complete-file-upload
         "1.0.0" (ch/create-complete-file-upload-cmd-handler
                  (complete-small-file-upload-creator c bucket)
                  (complete-multi-part-upload-creator c bucket)
                  filestore-upload-cache))
        (c/attach-validating-event-handler!
         communications
         :kixi.datastore/filestore-file-upload-completed
         :kixi.datastore.filestore/file-upload-completed
         "1.0.0" (eh/create-file-upload-completed-event-handler
                  filestore-upload-cache))
        (c/attach-validating-event-handler!
         communications
         :kixi.datastore/filestore-file-upload-failed
         :kixi.datastore.filestore/file-upload-failed
         "1.0.0" (eh/create-file-upload-failed-or-rejected-event-handler
                  filestore-upload-cache))
        (c/attach-validating-event-handler!
         communications
         :kixi.datastore/filestore-file-upload-rejected
         :kixi.datastore.filestore/file-upload-rejected
         "1.0.0" (eh/create-file-upload-failed-or-rejected-event-handler
                  filestore-upload-cache))
        (assoc component
               :creds
               c))
      component))
  (stop [component]
    (log/info "Stopping S3 FileStore")
    (if creds
      (dissoc component
              :creds)
      component)))
