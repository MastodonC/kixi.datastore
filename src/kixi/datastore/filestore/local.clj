(ns kixi.datastore.filestore.local
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as fs :refer [FileStore]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn create-link
  [dir]
  (fn [id]
    (str "file://" dir "/" id)))

(defn file-exists
  [dir]
  (fn [id]
    (let [^java.io.File file (io/file dir
                                      id)]
      (.exists file))))

(defn file-size
  [dir]
  (fn [id]
    (when-let [^java.io.File file (io/file dir
                                           id)]
      (when (.exists file)
        (.length file)))))

(defrecord Local
    [communications base-dir ^java.io.File dir]
    FileStore
    (exists [this id]
      ((file-exists dir) id))
    (size [this id]
      ((file-size dir) id)) ;;TODO tidy this
    (retrieve [this id]
      (let [^java.io.File file (io/file dir
                                        id)]
        (when (.exists file)
          (io/input-stream file))))

    component/Lifecycle
    (start [component]
      (if-not dir
        (let [dir (io/file (str (System/getProperty "java.io.tmpdir") base-dir))]
          (.mkdirs dir)
          (fs/attach-command-handlers communications
                                      {:link-creator (create-link dir)})
          (assoc component :dir dir))
        component))
    (stop [component]
      (info "Destroying Local File Datastore" component)
      (if dir
        (do (doseq [^java.io.File f (.listFiles dir)]
              (.delete f))
            (.delete dir)
            (dissoc component :dir))
        component)))
