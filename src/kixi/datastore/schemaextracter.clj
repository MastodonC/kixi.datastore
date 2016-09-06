(ns kixi.datastore.schemaextracter
  (:require [byte-streams :as bs]
            [clojure-csv.core :as csv :refer [parse-csv]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications.communications
             :refer [attach-pipeline-processor
                     detach-processor]]
            [kixi.datastore.documentstore.documentstore
             :refer [retrieve]]
            [taoensso.timbre :as timbre :refer [error info infof]])
  (:import [java.io File]))

(defn metadata->file
  [documentstore metadata]
  (let [id (get-in metadata [:file :id])
        f (File/createTempFile id ".tmp")]
    (.deleteOnExit f)
    (bs/transfer
     (retrieve documentstore id)
     f)
    f))

(defn requires-schema-extraction?
  [metadata]
  (and
   (get-in metadata [:structual-validation :valid])
   ((complement :schema) metadata)))

(defn extract-schema
  [documentstore]
  (fn [metadata]
    (let [^File file (metadata->file documentstore metadata)]
      (try
        (assoc metadata
               :schema true)
        (finally
          (.delete file))))))

(defprotocol ISchemaExtracter)

(defrecord SchemaExtracter
    [communications documentstore extract-schema-fn]
    ISchemaExtracter
    component/Lifecycle
    (start [component]
      (if-not extract-schema-fn
        (let [es-fn (extract-schema documentstore)]
          (info "Starting SchemaExtracter")
          (attach-pipeline-processor communications
                                     requires-schema-extraction?
                                     es-fn)
          (assoc component
                 :extract-schema-fn es-fn))
        component))
    (stop [component]
      (info "Stopping SchemaExtracter")
      (if extract-schema-fn
        (do
          (detach-processor communications
                           extract-schema-fn)
          (dissoc component 
                  :extract-schema-fn))
        component)))
