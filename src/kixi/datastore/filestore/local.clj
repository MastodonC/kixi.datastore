(ns kixi.datastore.filestore.local
  (:require [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :refer [FileStore]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defrecord Local
    [base-dir ^java.io.File dir]
    FileStore
    (output-stream [this id]
      (let [^java.io.File file (io/file base-dir 
                                        id)
            _ (.createNewFile file)]
        (io/output-stream file)))
    (retrieve [this id]      
      (let [^java.io.File file (io/file base-dir
                                        id)]
        (when (.exists file)
          (io/input-stream file))))

    component/Lifecycle
    (start [component]
      (info "Starting Local File Datastore")
      (let [dir (io/file base-dir)]
        (.mkdirs dir)
        (assoc component :dir dir)))
    (stop [component]
      (info "Destroying Local File Datastore")
      (doseq [^java.io.File f (.listFiles dir)]
        (.delete f))
      (.delete dir)
      (dissoc component :base-dir)))
