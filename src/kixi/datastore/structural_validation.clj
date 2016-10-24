(ns kixi.datastore.structural-validation
  (:require [byte-streams :as bs]
            [clojure-csv.core :as csv :refer [parse-csv]]
            [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.comms :as c
             :refer [attach-event-handler!]]
            [kixi.datastore.filestore
             :refer [retrieve]]
            [kixi.datastore.file
             :refer [temp-file]]
            [kixi.datastore.schemastore
             :as ss]
            [kixi.datastore.schemastore.validator
             :as sv]
            [kixi.datastore.metadatastore :as ms]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [clojure.java.io :as io])
  (:import [java.io File]))

(defn metadata->file
  [filestore metadata]
  (let [id (::ms/id metadata)
        f (temp-file id)]
    (bs/transfer
     (retrieve filestore id)
     f)
    f))

(defn csv-schema-test
  [schemastore schema-id file]
  (let [line-count (atom 0)]
    (try
      (with-open [contents (io/reader file)]
        (let [parser (parse-csv contents :strict true)
              explains (keep (fn [line]
                               (swap! line-count inc)
                               (sv/explain-data schemastore schema-id line))
                             (rest parser))]
          (if (seq explains)
            {::ms/valid false
             ::ms/explain (doall (take 100 explains))} ;This should be some sort of %age of file size
            {::ms/valid true})))
      (catch Exception e
        {::ms/valid false
         :line (inc @line-count)
         :e e}))))

(defn structural-validator
  [filestore schemastore]
  (fn [metadata]
    (let [^File file (metadata->file filestore metadata)]
      (try
        {:kixi.comms.event/key :kixi.datastore/file-metadata-updated
         :kixi.comms.event/version "1.0.0"
         :kixi.comms.event/payload {::ms/file-metadata-update-type ::ms/file-metadata-structural-validation-checked
                                    ::ms/id (::ms/id metadata)
                                    ::ms/structural-validation
                                    (case (::ms/type metadata)
                                      "csv" (csv-schema-test schemastore (::ss/id metadata) file))}}
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
          (attach-event-handler! communications
                                 :kixi.datastore/structural-validator
                                 :kixi.datastore/file-created
                                 "1.0.0"
                                 (comp sv-fn :kixi.comms.event/payload))
          (assoc component
                 :structural-validator-fn sv-fn))
        component))
    (stop [component]
      (info "Stopping Structural Validator")
      (when structural-validator-fn
        (dissoc component
                :structural-validator-fn)
        component)))
