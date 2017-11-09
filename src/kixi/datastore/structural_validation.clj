(ns kixi.datastore.structural-validation
  (:require [byte-streams :as bs]
            [clojure-csv.core :as csv :refer [parse-csv]]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as component]
            [kixi.comms :as c
             :refer [attach-event-handler!]]
            [kixi.datastore.communication-specs :as cs]
            [kixi.datastore.filestore
             :refer [retrieve]]
            [kixi.datastore.file
             :refer [temp-file]]
            [kixi.datastore.schemastore
             :as ss]
            [kixi.datastore.metadatastore
             :as ms]
            [kixi.datastore.schemastore.validator
             :as sv]
            [kixi.datastore.metadatastore :as ms]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [clojure.java.io :as io])
  (:import [java.io File]))

(defn metadata->file
  [filestore metadata]
  (let [id (::ms/id metadata)
        ^File f (temp-file id)]
    (if (.exists f)
      (do (bs/transfer
           (retrieve filestore id)
           f)
          f)
      (error "File does exist: " id))))

(def max-errors 10)  ;Maybe this should be some sort of %age of file size

(defn csv-schema-test
  [schema file header]
  (try
    (with-open [contents (io/reader file)]
      (let [lines' (line-seq contents)
            lines (if header
                     (rest lines')
                     lines')
            invalids (into [] (comp (map #(parse-csv % :strict true))
                                    (map first)
                                    (remove #(s/valid? schema %))
                                    (take max-errors)) lines)]
        (if (first invalids)
          {::ms/valid false
           ::ms/explain (map #(s/explain-str schema %) invalids)}
          {::ms/valid true})))
    (catch Exception e
      {::ms/valid false
       :e e})))

(defn structural-validator
  [filestore schemastore]
  (fn [metadata]
    (when (::ms/schema metadata)
      (when-let [^File file (metadata->file filestore metadata)]
        (try
          (let [schema (sv/schema-id->schema schemastore (get-in metadata [::ms/schema ::ss/id]))
                result (case (::ms/file-type metadata)
                         "csv" (csv-schema-test schema file (::ms/header metadata)))]
            {:kixi.comms.event/key :kixi.datastore.file-metadata/updated
             :kixi.comms.event/version "1.0.0"
             :kixi.comms.event/payload {::cs/file-metadata-update-type ::cs/file-metadata-structural-validation-checked
                                        ::ms/id (::ms/id metadata)
                                        ::ms/structural-validation result}
             :kixi.comms.event/partition-key (::ms/id metadata)})
          (finally
            (.delete file)))))))

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
                                 :kixi.datastore.file/created
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
