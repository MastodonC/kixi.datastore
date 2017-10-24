(ns kixi.datastore.filestore.local
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore]]
            [kixi.datastore.filestore.command-handler :as ch]
            [kixi.comms :as c]
            [taoensso.timbre :as timbre :refer [error info infof]]))

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

(defn file-exists
  [dir id]
  (let [^java.io.File file (io/file dir id)]
    (.exists file)))

(defn file-size
  [dir id]
  (when-let [^java.io.File file (io/file dir id)]
    (when (.exists file)
      (.length file))))

(defn multi-part-upload-creator
  [dir]
  (fn [id part-count]
    (let [upload-id (uuid)
          links (vec (doall
                      (for [i (range 1 (inc part-count))]
                        (str ((create-link dir) upload-id) "-" i))))]
      {:upload-id upload-id
       :upload-part-urls links})))

(defn complete-multi-part-upload-creator
  [dir]
  (fn [id etags upload-id]
    (let [new-file-path (-> ((create-link dir) id)
                            (remove-file-path-prefix))]
      (with-open [o (io/output-stream new-file-path)]
        (run! #(let [path (-> ((create-link dir) upload-id)
                              (str "-" %)
                              (remove-file-path-prefix))]
                 (io/copy (io/file path) o)) (range 1 (inc (count etags))))))))

(defrecord Local
    [communications base-dir ^java.io.File dir]
  FileStore
  (exists [this id]
    (file-exists dir id))
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
        (.mkdirs dir)
        (c/attach-command-handler!
         communications
         :kixi.datastore/filestore
         :kixi.datastore.filestore/create-upload-link
         "1.0.0" (ch/create-upload-cmd-handler (create-link dir)))
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-multi-part
         :kixi.datastore.filestore/create-multi-part-upload-link
         "1.0.0" (ch/create-multi-part-upload-cmd-handler (multi-part-upload-creator dir)))
        (c/attach-validating-command-handler!
         communications
         :kixi.datastore/filestore-multi-part-completed
         :kixi.datastore.filestore/complete-multi-part-upload
         "1.0.0" (ch/create-complete-multi-part-upload-cmd-handler (complete-multi-part-upload-creator dir)))
        (assoc component :dir dir))
      component))
  (stop [component]
    (info "Destroying Local File Datastore")
    (if dir
      (do (doseq [^java.io.File f (.listFiles dir)]
            (.delete f))
          (.delete dir)
          (dissoc component :dir))
      component)))
