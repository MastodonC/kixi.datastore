(ns kixi.datastore.documentstore.local
  (:require [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [kixi.datastore.protocols :refer [DocumentStore]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(def meta-data->file-name :name)

(defrecord Local
    [base-dir]
    DocumentStore
    (output-stream [this meta-data ]
      (let [^java.io.File file (io/file base-dir 
                                        (meta-data->file-name meta-data))
            _ (.createNewFile file)]
        (io/output-stream file)))
    (retrieve [this file-meta-data])

    component/Lifecycle
    (start [component]
      (let [dir (io/file base-dir)]
        (.mkdirs dir)
        (assoc component :base-dir dir)))
    (stop [component]
      (doseq [f (.listFiles base-dir)]
        (.delete f))
      (.delete base-dir)
      (dissoc component :base-dir)))
