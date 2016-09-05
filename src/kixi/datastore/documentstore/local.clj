(ns kixi.datastore.documentstore.local
  (:require [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [kixi.datastore.protocols :refer [DocumentStore]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(def meta-data->file-name :id)

(defrecord Local
    [base-dir ^java.io.File dir]
    DocumentStore
    (output-stream [this meta-data]
      (let [^java.io.File file (io/file base-dir 
                                        (meta-data->file-name meta-data))
            _ (.createNewFile file)]
        (io/output-stream file)))
    (retrieve [this meta-data]
      (let [^java.io.File file (io/file base-dir 
                                        (meta-data->file-name meta-data))]
        (when (.exists file)
          file)))

    component/Lifecycle
    (start [component]
      (info "Starting Local Datastore")
      (let [dir (io/file base-dir)]
        (.mkdirs dir)
        (assoc component :dir dir)))
    (stop [component]
      (info "Destroying Local Datastore")
      (doseq [^java.io.File f (.listFiles dir)]
        (.delete f))
      (.delete dir)
      (dissoc component :base-dir)))
