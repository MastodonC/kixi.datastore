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
        ^File f (temp-file id)]
    (if (.exists f)
      (do (bs/transfer
           (retrieve filestore id)
           f)
          f)
      (error "File does exist: " id))))

(defn csv-schema-test
  [schemastore schema-id file]
  (let [schema (sv/schema-id->schema schemastore schema-id)]
    (try
      (with-open [contents (io/reader file)]
        (let [lines (->> contents
                         line-seq
                         rest)          ;header line
              invalids (into [] (comp (take 10)
                                      (remove #(s/valid? schema %))
                                      (map #(parse-csv % :strict true))) lines)]
          (if (first invalids)
            {::ms/valid false
             ::ms/explain (map #(s/explain-data schema %) invalids)} ;This should be some sort of %age of file size
            {::ms/valid true})))
      (catch Exception e
        {::ms/valid false
         :e e}))))

(defn structural-validator
  [filestore schemastore]
  (fn [metadata]
    (when-let [^File file (metadata->file filestore metadata)]
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
