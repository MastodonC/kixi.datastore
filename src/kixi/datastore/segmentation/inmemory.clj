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
    (if-let [src (kdfs/retrieve filestore id)]
      (do (bs/transfer src
                       file)
          file)
      {::seg/reason :file-not-found
       ::seg/cause id})))

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
      (if dex
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
            (vals files)))
        {::seg/reason :invalid-column
         ::seg/cause column-name}))))

(defn upload-segment
  [filestore communications]
  (fn [basemetadata request segment-data]
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
      segment-data)))

(defmacro while-not->>
  "Threads body while the result of each statement is not spec. Returns interim value if valid? spec"
  [spec x & forms]
  `(try
     ~(loop [x x forms forms]
      (if forms
        (let [form (first forms)
              threaded (if (seq? form)
                         (with-meta `(~(first form) ~@(next form)  ~x) (meta form))
                         (list form x))
              valid-threaded `(let [result# ~threaded]
                                (if (s/valid? ~spec result#) 
                                  (throw (ex-info "valid" result#))
                                  result#))]
          (recur valid-threaded (next forms)))
        x))
     (catch Exception e# (ex-data e#))))

(defn group-rows-by-column-csv
  [uploader retrieve-file request metadata]
  (while-not->> ::seg/error
      (::ms/id request)
      retrieve-file
      (segmentate-file-by-column-values (::seg/column-name request))                   
      (map (partial uploader metadata request))
      doall
      (map :id)))

(defn group-rows-by-column  
  [uploader retrieve-file request metadata]
  (case (::ms/type metadata)
    "csv" (group-rows-by-column-csv uploader retrieve-file request metadata)
    {::seg/reason :unknown-file-type
     ::seg/cause (::ms/type metadata)}))

(defn segmentation-processor
  [filestore metadatastore communications]
  (let [segment-uploader (upload-segment filestore communications)
        file-retriever (partial retrieve-file-to-local filestore)]
    (fn [request]
      (let [metadata (ms/fetch metadatastore (::ms/id request))
            result (case (:kixi.datastore.request/type request)
                     ::seg/group-rows-by-column (group-rows-by-column segment-uploader file-retriever request metadata))]
        (prn "RRR: " result)
        (->> (if (s/valid? ::seg/error result)
               {::seg/created false
                :kixi.datastore.request/request request
                ::seg/error result}
               {::seg/created true
                ::seg/segment-ids result
                :kixi.datastore.request/request request})
             vector
             (update metadata ::ms/segmentations concat))))))

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
                                   (segmentation-processor filestore metadatastore communications))
      component)
    (stop [component]))
