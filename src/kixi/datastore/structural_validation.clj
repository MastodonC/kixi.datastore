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
        f (File/createTempFile id ".tmp")]
    (.deleteOnExit f)
    (bs/transfer
     (retrieve filestore id)
     f)
    f))

(defn csv-structural-validator
  [file]
  (let [line-count (atom 0)]
    (try
      (with-open [contents (io/reader file)]
        (doseq [line (parse-csv contents :strict true)]
          (swap! line-count inc)))
      {:valid true}
      (catch Exception e
        {:valid false
         :line (inc @line-count)
         :e e}))))

(defn structural-validator
  [filestore]
  (fn [metadata]
    (let [^File file (metadata->file filestore metadata)]
      (try
        (assoc metadata
               :structural-validation
               (case (::ms/type metadata)
                 "csv" (csv-structural-validator file)))
        (finally
          (.delete file))))))

(defprotocol IStructuralValidator)

(defrecord StructuralValidator
    [communications filestore structural-validator-fn]
    IStructuralValidator
    component/Lifecycle
    (start [component]
      (if-not structural-validator-fn
        (let [sv-fn (structural-validator filestore)]
          (info "Starting Structural Validator")
          (attach-pipeline-processor communications
                                     requires-structural-validation?
                                     (structural-validator filestore))
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
