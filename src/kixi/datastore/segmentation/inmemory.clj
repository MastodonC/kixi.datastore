(ns kixi.datastore.segmentation.inmemory
  (:require [byte-streams :as bs]
            [com.stuartsierra.component :as component]
            [clj-time.core :refer [now]]
            [clojure.java.io :as io]
            [clojure.spec :as s]
            [clojure-csv.core :as csv :refer [parse-csv write-csv]]
            [kixi.datastore.segmentation :as seg :refer [Segmentation]]
            [kixi.comms :refer [Communications attach-event-handler!] :as comms]
            [kixi.datastore.communication-specs :as cs]
            [kixi.datastore.filestore :as kdfs]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as ms]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [kixi.datastore.communications :as c]
            [kixi.datastore.file :refer [temp-file close]]))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn retrieve-file-to-local
  [filestore id]
  (let [file (temp-file id)]
    (if-let [src (kdfs/retrieve filestore id)]
      (do (bs/transfer src
                       file)
          file)
      {::seg/reason :file-not-found
       ::seg/cause id})))

(defn new-segment-data
  [headers value]
  (let [id (uuid)
        file (temp-file id)
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
        (->> (rest parser)
             (reduce
              (fn [value->segment-data line]
                (let [value (nth line dex)
                      segment-data (get value->segment-data value
                                        (new-segment-data header-line value))
                      line-csv (write-csv [line])]
                  (bs/transfer line-csv
                               (:output-stream segment-data)
                               {:close? false
                                :append? true})
                  (assoc value->segment-data
                         value
                         (-> segment-data
                             (update :lines
                                     inc)
                             (update :size-bytes
                                     (partial + (alength (.getBytes ^String line-csv))))))))
              {})
             vals
             (map (fn [segment-data]
                    (close (:output-stream segment-data))
                    segment-data)))
        {::seg/reason :invalid-column
         ::seg/cause column-name}))))

(defn upload-segment
  [filestore communications]
  (fn [basemetadata request segment-data]
    (bs/transfer (:file segment-data)
                 (kdfs/output-stream filestore (:id segment-data)))
    (let [metadata (assoc (select-keys basemetadata
                                       [::ms/type
                                        ::ms/name
                                        ::ss/id
                                        ::ms/header
                                        ::ms/sharing])
                          ::ms/id (:id segment-data)
                          ::ms/size-bytes (:size-bytes segment-data)
                          ::ms/provenance {::ms/source "segmentation"
                                           ::ms/parent-id (::ms/id basemetadata)
                                           :kixi.user/id (:kixi.user/id request)}
                          ::seg/segment {::seg/request request
                                         ::seg/line-count (:lines segment-data)
                                         ::seg/value (:value segment-data)})]
      (cs/send-event! communications
                      (assoc metadata
                             ::cs/event :kixi.datastore/file-created
                             ::cs/version "1.0.0"))
      (cs/send-event! communications
                      {::cs/event :kixi.datastore/file-metadata-updated
                       ::cs/version "1.0.0"
                       ::cs/file-metadata-update-type ::cs/file-metadata-created
                       ::ms/file-metadata metadata}))
    segment-data))

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
      (let [metadata (ms/retrieve metadatastore (::ms/id request))
            result (case (:kixi.datastore.request/type request)
                     ::seg/group-rows-by-column (group-rows-by-column segment-uploader file-retriever request metadata))]
        (->> (if (s/valid? ::seg/error result)
               {::seg/created false
                :kixi.datastore.request/request request
                ::seg/error result}
               {::seg/created true
                :kixi.datastore.request/request request
                ::seg/segment-ids result})
             (hash-map ::cs/file-metadata-update-type ::cs/file-metadata-segmentation-add ::ms/id (::ms/id request) ::ms/segmentation)
             (hash-map :kixi.comms.event/key :kixi.datastore/file-metadata-updated :kixi.comms.event/version "1.0.0" :kixi.comms.event/payload))))))

(comment "There is something horrible happening when target file has invalid metadata, seems to end up with the entire consumer config in the payload.
Not a problem for now, but when productionising this code, investigate!")

(defrecord InMemory
    [communications filestore metadatastore]
    Segmentation
    component/Lifecycle
    (start [component]
      (info "Starting InMemory Segmentation Processor")
      (attach-event-handler! communications
                             :segmentation
                             :kixi.datastore/file-segmentation-created
                             "1.0.0"
                             (comp (segmentation-processor filestore metadatastore communications) :kixi.comms.event/payload))
      component)
    (stop [component]))
