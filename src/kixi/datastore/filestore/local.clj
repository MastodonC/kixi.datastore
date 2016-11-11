(ns kixi.datastore.filestore.local
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async :refer [go]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :refer [FileStore]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defrecord Local
    [base-dir ^java.io.File dir]
    FileStore
    (exists [this id]
      (let [^java.io.File file (io/file dir
                                        id)]
        (.exists file)))
    (output-stream [this id content-length]
      (let [^java.io.File file (io/file dir 
                                        id)
            _ (.createNewFile file)]
        [(go :done)
         (io/output-stream file)]))
    (retrieve [this id]      
      (let [^java.io.File file (io/file dir
                                        id)]
        (when (.exists file)
          (io/input-stream file))))

    component/Lifecycle
    (start [component]
      (info "Starting Local File Datastore")
      (let [dir (io/file (str (System/getProperty "java.io.tmpdir") base-dir))]
        (.mkdirs dir)
        (assoc component :dir dir)))
    (stop [component]
      (info "Destroying Local File Datastore" component)
      (if dir
        (do (doseq [^java.io.File f (.listFiles dir)]
              (.delete f))
            (.delete dir)
            (dissoc component :dir))
        component)))
