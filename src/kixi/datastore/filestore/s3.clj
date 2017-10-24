(ns kixi.datastore.filestore.s3
  (:require [amazonica.core :as aws]
            [amazonica.aws.s3 :as s3]
            [amazonica.aws.s3transfer :as s3t]
            [byte-streams :as bs]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore]]
            [kixi.datastore.filestore.command-handler :as ch]
            [kixi.datastore.time :as t]
            [kixi.comms :as c]
            [taoensso.timbre :as timbre :refer [error]])
  (:import [com.amazonaws.services.s3.model AmazonS3Exception GeneratePresignedUrlRequest PartETag CompleteMultipartUploadRequest]))

(defn ensure-bucket
  [creds bucket]
  (when-not (s3/does-bucket-exist creds bucket)
    (s3/create-bucket creds bucket)))

(defn multi-part-upload-creator
  [creds bucket]
  (fn [id part-count]
    (let [{:keys [upload-id] :as initate-resp} (s3/initiate-multipart-upload creds
                                                                             :bucket-name bucket
                                                                             :key id)
          client (com.amazonaws.services.s3.AmazonS3ClientBuilder/defaultClient)
          links (vec (doall
                      (for [i (range 1 (inc part-count))]
                        (let [^org.joda.time.DateTime exp (t/minutes-from-now (* 60 24)) ;; 24hrs
                              req (doto (GeneratePresignedUrlRequest. bucket id com.amazonaws.HttpMethod/PUT)
                                    (.setExpiration (.toDate exp))
                                    (.addRequestParameter "partNumber" (str i))
                                    (.addRequestParameter "uploadId" upload-id))]
                          (str (.generatePresignedUrl client req))))))]
      {:upload-id upload-id
       :upload-part-urls links})))

(defn complete-multi-part-upload-creator
  [creds bucket]
  (fn [id etags upload-id]
    (let [req (CompleteMultipartUploadRequest. bucket id upload-id (map-indexed #(PartETag. (inc %1) %2) etags))
          client (com.amazonaws.services.s3.AmazonS3ClientBuilder/defaultClient)]
      (.completeMultipartUpload client req))))

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

(defrecord S3
    [communications logging region endpoint access-key secret-key link-expiration-mins bucket client-options creds]
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
        (ensure-bucket c bucket)
        (c/attach-command-handler!
         communications
         :kixi.datastore/filestore
         :kixi.datastore.filestore/create-upload-link
         "1.0.0" (ch/create-upload-cmd-handler (create-link c bucket)))
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-multi-part
         :kixi.datastore.filestore/create-multi-part-upload-link
         "1.0.0" (ch/create-multi-part-upload-cmd-handler (multi-part-upload-creator c bucket)))
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-multi-part-completed
         :kixi.datastore.filestore/complete-multi-part-upload
         "1.0.0" (ch/create-complete-multi-part-upload-cmd-handler (complete-multi-part-upload-creator c bucket)))
        (assoc component
               :creds
               c))
      component))
  (stop [component]
    (if creds
      (dissoc component
              :creds)
      component)))
