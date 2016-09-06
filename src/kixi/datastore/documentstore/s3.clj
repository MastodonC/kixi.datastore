(ns kixi.datastore.documentstore.s3
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.documentstore.documentstore :refer [DocumentStore]]))

(defrecord S3
    [region bucket key-prefix]
    DocumentStore
    (output-stream [this file-meta-data])
    (retrieve [this file-meta-data])
    component/Lifecycle
    (start [component]
      component)
    (stop [component]
      component))
