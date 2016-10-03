(ns kixi.datastore.structural-validation
  (:require [byte-streams :as bs]
            [clojure-csv.core :as csv :refer [parse-csv]]
            [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications
             :refer [attach-pipeline-processor
                     detach-processor]]
            [kixi.datastore.filestore
             :refer [retrieve]]
            [kixi.datastore.file
             :refer [temp-file]]
            [kixi.datastore.schemastore
             :as ss]
            [kixi.datastore.metadatastore :as ms]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [clojure.java.io :as io])
  (:import [java.io File]))


(defn requires-structural-validation?
  [msg]
  (and (s/valid? ::ms/filemetadata msg)
       ((complement :structural-validation) msg)))

(defn metadata->file
  [filestore metadata]
  (let [id (::ms/id metadata)
        f (temp-file id)]
    (bs/transfer
     (retrieve filestore id)
     f)
    f))

(defn metadata->schema
  [schemastore metadata]
  (ss/fetch-spec schemastore (::ss/name metadata)))

(defn csv-schema-test
  [schema file]
  (let [line-count (atom 0)]
    (try
      (with-open [contents (io/reader file)]
        (let [parser (parse-csv contents :strict true)
              explains (keep (fn [line]
                               (swap! line-count inc)
                               (s/explain-data schema line))
                             (rest parser))]
          (if (seq explains)
            {:valid false
             :explain (doall (take 100 explains))} ;This should be some sort of %age of file size
            {:valid true})))
      (catch Exception e
        {:valid false
         :line (inc @line-count)
         :e e}))))

(defn structural-validator
  [filestore schemastore]
  (fn [metadata]
    (let [^File file (metadata->file filestore metadata)
          schema (metadata->schema schemastore metadata)]
      (try
        (assoc metadata
               :structural-validation
               (case (::ms/type metadata)
                 "csv" (csv-schema-test schema file)))
        (finally
          (.delete file))))))

(defprotocol IStructuralValidator)

(defrecord StructuralValidator
    [communications filestore schemastore structural-validator-fn]
    IStructuralValidator
    component/Lifecycle
    (start [component]
      (if-not structural-validator-fn
        (let [sv-fn (structural-validator filestore schemastore)]
          (info "Starting Structural Validator")
          (attach-pipeline-processor communications
                                     requires-structural-validation?
                                     sv-fn)
          (assoc component
                 :structural-validator-fn sv-fn))
        component))
    (stop [component]
      (info "Stopping Structural Validator")
      (if structural-validator-fn
        (do
          (detach-processor communications
                            structural-validator-fn)
          (dissoc component 
                  :structural-validator-fn))
        component)))
