(ns kixi.datastore.structural-validation
  (:require [byte-streams :as bs]
            [clojure-csv.core :as csv :refer [parse-csv]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications
             :refer [attach-pipeline-processor
                     detach-processor]]
            [kixi.datastore.documentstore
             :refer [retrieve]]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [clojure.java.io :as io])
  (:import [java.io File]))


(defn requires-structural-validation?
  [metadata]
  ((complement :structural-validation) metadata))


(defn metadata->file
  [documentstore metadata]
  (let [id (:id metadata)
        f (File/createTempFile id ".tmp")]
    (.deleteOnExit f)
    (bs/transfer
     (retrieve documentstore id)
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
  [documentstore]
  (fn [metadata]
    (let [^File file (metadata->file documentstore metadata)]
      (try
        (assoc metadata
               :structural-validation
               (case (:type metadata)
                 :csv (csv-structural-validator file)))
        (finally
          (.delete file))))))

(defprotocol IStructuralValidator)

(defrecord StructuralValidator
    [communications documentstore structural-validator-fn]
    component/Lifecycle
    (start [component]
      (if-not structural-validator-fn
        (let [sv-fn (structural-validator documentstore)]
          (info "Starting Structural Validator")
          (attach-pipeline-processor communications
                                     requires-structural-validation?
                                     (structural-validator documentstore))
          (assoc component
                 :structural-validator-fn sv-fn))))
    (stop [component]
      (info "Stopping Structural Validator")
      (if structural-validator-fn
        (do
          (detach-processor communications
                            structural-validator-fn)
          (dissoc component 
                  :structural-validator-fn))
        component)))
