(ns kixi.datastore.filestore.local
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore FileStoreUploadCache]]
            [kixi.datastore.filestore.command-handler :as ch]
            [kixi.datastore.filestore.event-handler :as eh]
            [kixi.comms :as c]
            [taoensso.timbre :as log :refer [error info infof]]))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn create-link
  [dir]
  (fn [id]
    (str "file://" dir "/" id)))

(defn remove-file-path-prefix
  [p]
  (subs p 7))

(defn file-exists?
  [dir id]
  (let [^java.io.File file (io/file dir id)]
    (.exists file)))

(defn file-size
  [dir id]
  (when-let [^java.io.File file (io/file dir id)]
    (when (.exists file)
      (.length file))))

(defn init-multi-part-upload-creator
  [dir]
  (fn [id part-ranges]
    (let [links (vec (map-indexed (fn [i p]
                                    (assoc p :url (str ((create-link dir) id) "-" i))) part-ranges))]
      {:upload-id (uuid)
       :upload-parts links})))

(defn complete-multi-part-upload-creator
  [dir]
  (fn [id etags _]
    (try (let [new-file-path (-> ((create-link dir) id)
                                 (remove-file-path-prefix))]
           (with-open [o (io/output-stream new-file-path)]
             (run! #(let [path (-> ((create-link dir) id)
                                   (str "-" %)
                                   (remove-file-path-prefix))]
                      (io/copy (io/file path) o)) (range (count etags)))))
         [true nil nil]
         (catch java.io.FileNotFoundException _
           [false :file-missing nil]))))

(defn complete-small-file-upload-creator
  [dir]
  (fn [id part-ids _]
    (if (file-exists? dir id)
      [true nil nil]
      [false :file-missing nil])))

(defrecord Local
    [communications filestore-upload-cache base-dir
     ^java.io.File dir]
  FileStore
  (exists [this id]
    (file-exists? dir id))
  (size [this id]
    (file-size dir id))
  (retrieve [this id]
    (let [^java.io.File file (io/file dir id)]
      (when (.exists file)
        (io/input-stream file))))
  (create-link [this id file-name]
    (let [^java.io.File file (io/file dir id)]
      (when (.exists file)
        (str "file://" (.getPath file)))))

  component/Lifecycle
  (start [component]
    (if-not dir
      (let [dir (io/file (str (System/getProperty "java.io.tmpdir") base-dir))]
        (log/info "Starting Local FileStore - dir:" dir)
        (.mkdirs dir)
        ;; LEGACY
        (c/attach-command-handler!
         communications
         :kixi.datastore/filestore
         :kixi.datastore.filestore/create-upload-link
         "1.0.0" (ch/create-upload-cmd-handler (create-link dir)))
        ;; NEW
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-initiate-file-upload
         :kixi.datastore.filestore/initiate-file-upload
         "1.0.0" (ch/create-initiate-file-upload-cmd-handler
                  (create-link dir)
                  (init-multi-part-upload-creator dir)
                  filestore-upload-cache))
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-complete-file-upload
         :kixi.datastore.filestore/complete-file-upload
         "1.0.0" (ch/create-complete-file-upload-cmd-handler
                  (complete-small-file-upload-creator dir)
                  (complete-multi-part-upload-creator dir)
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
               :dir dir))
      component))
  (stop [component]
    (info "Stopping Local FileStore")
    (if dir
      (do (doseq [^java.io.File f (.listFiles dir)]
            (.delete f))
          (.delete dir)
          (dissoc component :dir))
      component)))
