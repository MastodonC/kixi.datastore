(ns kixi.datastore.segmentation.inmemory
  (:require [byte-streams :as bs]
            [com.stuartsierra.component :as component]
            [clj-time.core :refer [now]]
            [clojure.java.io :as io]
            [clojure.spec :as s]
            [clojure-csv.core :as csv :refer [parse-csv write-csv]]
            [kixi.datastore.segmentation :as seg :refer [Segmentation]]
            [kixi.datastore.communications :refer [Communications] :as comms]
            [kixi.datastore.filestore :as kdfs]
            [kixi.datastore.metadatastore :as ms]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [kixi.datastore.communications :as c]))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn retrieve-file-to-local
  [filestore id]
  (let [^java.io.File file (java.io.File/createTempFile id ".tmp")]
    (bs/transfer (kdfs/retrieve filestore id)
                 file)
    file))

(defn new-file
  [headers value]
  (let [id (uuid)
        ^java.io.File file (java.io.File/createTempFile id ".tmp")
        os (io/output-stream file)
        header-line (write-csv [headers])
        _ (bs/transfer header-line
                       os
                       {:close? false})]
    {:value value
     :file file
     :id id
     :lines 1
     :size-bytes (alength (.getBytes ^String header-line))
     :output-stream os}))

(defn index-of
  [coll value]
  (first
   (keep-indexed
    #(when (= value %2)
       %1)
    coll)))

(defn segmentate-file-by-column-values
  [column-name file]
  (with-open [contents (io/reader file)]
    (let [parser (parse-csv contents :strict true)
          header-line (first parser)
          dex (index-of header-line column-name)]
      (loop [files {}
             lines (rest parser)]
        (if-let [line (first lines)]
          (let [value (nth line dex)
                file (or (get files value)
                         (new-file header-line value))
                line-csv (write-csv [line])]
            (bs/transfer line-csv
                         (:output-stream file)
                         {:close? false
                          :append? true})
            (recur (assoc files
                          value
                          (-> file
                              (update :lines
                                      inc)
                              (update :size-bytes
                                      (partial + (alength (.getBytes ^String line-csv))))))
                   (rest lines)))
          (vals files))))))

(defn upload-segment
  [filestore communications basemetadata request] 
  (fn [segment-data]
    (let [_ (.close ^java.io.OutputStream (:output-stream segment-data))
          _ (bs/transfer (:file segment-data)
                         (kdfs/output-stream filestore (:id segment-data)))
          _ (comms/submit communications 
                          (assoc (select-keys basemetadata 
                                              [::ms/type
                                               ::ms/name
                                               ::kixi.datastore.schemastore/id]) 
                                 ::ms/id (:id segment-data)
                                 ::ms/size-bytes (:size-bytes segment-data)
                                 ::ms/provanance {::ms/source "segmentation"
                                                  ::ms/parent-id (::ms/id basemetadata)}
                                 ::seg/segment {::seg/request request
                                                ::seg/line-count (:lines segment-data)
                                                ::seg/value (:value segment-data)}))]
      (:id segment-data))))



(defn column-segmentation-request-processor
  [filestore metadatastore communications]
  (fn [request]
    (prn "RRR: " request)
    (when-let [metadata (ms/fetch metadatastore (::ms/id request))]
      (prn "MM: " metadata)  
      (case (::ms/type metadata)
        "csv" (->> (::ms/id request)
                   (retrieve-file-to-local filestore)
                   (segmentate-file-by-column-values (::seg/column-name request))                   
                   (map (upload-segment filestore communications metadata request))
                   doall
                   (hash-map ::seg/created true :kixi.datastore.request/request request ::seg/segment-ids)
                   vector
                   (update metadata ::ms/segmentations concat))
        (update metadata
                ::ms/segmentations
                concat
                [{::seg/created false
                  ::seg/request request
                  ::seg/msg :unknown-file-type}])))))

(defrecord InMemory
    [communications filestore metadatastore]
    Segmentation
    component/Lifecycle
    (start [component]
      (info "Starting InMemory Segmentation Processor")
      (c/attach-pipeline-processor communications
                                   #(and 
                                         (s/valid? :kixi.datastore.request/request %)
                                         (s/valid? ::seg/type (:kixi.datastore.request/type %)))
                                   (column-segmentation-request-processor filestore metadatastore communications))
      component)
    (stop [component]))
