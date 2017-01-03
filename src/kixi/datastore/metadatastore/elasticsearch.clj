(ns kixi.datastore.metadatastore.elasticsearch
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore
             [communication-specs :as cs]
             [elasticsearch :as es :refer [ensure-index string-stored-not_analyzed string-analyzed]]
             [metadatastore :as ms :refer [MetaDataStore]]
             [schemastore :as ss]
             [segmentation :as seg]]
            [taoensso.timbre :as timbre :refer [info]]))

(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

(def doc-def
  {::ms/id string-stored-not_analyzed
   ::ms/type string-stored-not_analyzed
   ::ms/name string-analyzed
   ::ms/schema {:properties {::ss/id string-stored-not_analyzed
                             ::ms/added es/timestamp}}
   ::ms/provenance {:properties {::ms/source string-stored-not_analyzed
                                 :kixi.user/id string-stored-not_analyzed
                                 ::ms/parent-id string-stored-not_analyzed
                                 ::ms/created es/timestamp}}
   ::ms/segmentation {:type "nested"
                      :properties {::seg/type string-stored-not_analyzed
                                   ::seg/line-count es/long
                                   ::seg/value string-stored-not_analyzed}}
   ::ms/sharing {:properties (zipmap ms/activities
                                     (repeat string-stored-not_analyzed))}})

(s/fdef update-metadata-processor
        :args (s/cat :conn #(instance? clojurewerkz.elastisch.rest.Connection %)
                     :update-req ::cs/file-metadata-updated))

(def merge-data
  (partial es/merge-data index-name doc-type))

(def cons-data
  (partial es/cons-data index-name doc-type))

(def get-document
  (partial es/get-document index-name doc-type))

(def get-document-key
  (partial es/get-document-key index-name doc-type))

(def present?
  (partial es/present? index-name doc-type))

(def search
  (partial es/search-data index-name doc-type))

(defmulti update-metadata-processor
  (fn [conn update-event]
    (::cs/file-metadata-update-type update-event)))

(defmethod update-metadata-processor ::cs/file-metadata-created
  [conn update-event]
  (let [metadata (::ms/file-metadata update-event)]
    (info "Update: " metadata)
    (merge-data
     conn
     (::ms/id metadata)
     metadata)))

(defmethod update-metadata-processor ::cs/file-metadata-structural-validation-checked
  [conn update-event]
  (info "Update: " update-event)
  (merge-data
   conn
   (::ms/id update-event)
   (select-keys update-event
                [::ms/structural-validation])))

(defmethod update-metadata-processor ::cs/file-metadata-segmentation-add
  [conn update-event]
  (info "Update: " update-event)
  (cons-data
   conn
   (::ms/id update-event)
   ::ms/segmentations
   (::ms/segmentation update-event)))

(defn response-event
  [r]
  nil)

(s/fdef query-criteria->es-query
        :args (s/cat :criteria ::ms/query-criteria))

(defn query-criteria->es-query
  [criteria]
  (let [activities (->> criteria
                        ::ms/activities
                        (cons ::ms/meta-read)
                        set)
        groups (:kixi.user/groups criteria)]
    {::ms/sharing (zipmap activities
                          (repeat groups))}))

(defrecord ElasticSearch
    [communications host port conn]
    MetaDataStore
    (authorised
      [this action id user-groups]
      (when-let [sharing (get-document-key conn id ::ms/sharing)]
        (not-empty (clojure.set/intersection (set (get sharing action))
                                             (set user-groups)))))
    (exists [this id]
      (present? conn id))
    (retrieve [this id]
      (get-document conn id))
    (query [this criteria from-index count]
      (search conn 
              (query-criteria->es-query criteria)
              from-index count))

    component/Lifecycle
    (start [component]
      (if-not conn
        (let [connection (es/connect host port)]
          (info "Starting File Metadata ElasticSearch Store")
          (ensure-index index-name
                        doc-type
                        doc-def
                        connection)
          (c/attach-event-handler! communications
                                   :kixi.datastore/metadatastore
                                   :kixi.datastore.file-metadata/updated
                                   "1.0.0"
                                   (comp response-event (partial update-metadata-processor connection) :kixi.comms.event/payload))
          (assoc component :conn connection))
        component))
    (stop [component]
      (if conn
        (do (info "Destroying File Metadata ElasticSearch Store")
            (dissoc component :conn))
        component)))
