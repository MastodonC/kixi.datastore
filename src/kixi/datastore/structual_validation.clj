(ns kixi.datastore.structual-validation
  (:require [byte-streams :as bs]
            [clojure-csv.core :as csv :refer [parse-csv]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications.communications
             :refer [attach-pipeline-processor
                     detach-processor]]
            [kixi.datastore.documentstore.documentstore
             :refer [retrieve]]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [clojure.java.io :as io])
  (:import [java.io File]))


(defn requires-structual-validation?
  [metadata]
  ((complement :structual-validation) metadata))


(defn metadata->file
  [documentstore metadata]
  (let [id (get-in metadata [:file :id])
        f (File/createTempFile id ".tmp")]
    (.deleteOnExit f)
    (bs/transfer
     (retrieve documentstore id)
     f)
    f))

(defn csv-structual-validator
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

(defn structual-validator
  [documentstore]
  (fn [metadata]
    (let [^File file (metadata->file documentstore metadata)]
      (try
        (assoc metadata
               :structual-validation
               (case (:type metadata)
                 :csv (csv-structual-validator file)))
        (finally
          (.delete file))))))

(defprotocol IStructuralValidator)

(defrecord StructuralValidator
    [communications documentstore structual-validator-fn]
    component/Lifecycle
    (start [component]
      (if-not structual-validator-fn
        (let [sv-fn (structual-validator documentstore)]
          (info "Starting Structual Validator")
          (attach-pipeline-processor communications
                                     requires-structual-validation?
                                     (structual-validator documentstore))
          (assoc component
                 :structual-validator-fn sv-fn))))
    (stop [component]
      (info "Stopping Structual Validator")
      (if structual-validator-fn
        (do
          (detach-processor communications
                            structual-validator-fn)
          (dissoc component 
                  :structual-validator-fn))
        component)))


