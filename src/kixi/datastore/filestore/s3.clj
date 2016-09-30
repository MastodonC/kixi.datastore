(ns kixi.datastore.filestore.s3
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :refer [FileStore]]))

(defrecord S3
    [region bucket key-prefix]
    FileStore
    (output-stream [this file-meta-data])
    (retrieve [this file-meta-data])
    component/Lifecycle
    (start [component]
      component)
    (stop [component]
      component))
