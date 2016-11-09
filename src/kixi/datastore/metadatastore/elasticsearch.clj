(ns kixi.datastore.metadatastore.elasticsearch
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore
             :refer [MetaDataStore] :as ms]
            [kixi.comms :as c]
            [kixi.datastore.communication-specs :as cs]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.segmentation :as seg]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [kixi.datastore.schemastore :as ss]
            [medley.core :refer [map-keys]]))

(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

(def put-opts {:consistency "default"})

(defn kw->es-format
  [kw]
  (if (namespace kw)
    (str (clojure.string/replace (namespace kw) "." "_")
         "__"
         (name kw))
    (name kw)))

(defn es-format->kw
  [confused-kw]  
  (let [splits (clojure.string/split (name confused-kw) #"__")]
    (if (second splits)
      (keyword
       (clojure.string/replace (first splits) "_" ".")
       (second splits))
      confused-kw)))

(defn map-all-keys
  [f]
  (fn mapper [m]
    (cond
      (map? m) (zipmap (map f (keys m))
                       (map (fn [v]
                              (cond  
                                (map? v) (mapper v)
                                (list? v) (map mapper v) 
                                (vector? v) (mapv mapper v)
                                (seq? v) (map mapper v) 
                                (symbol? v) (name v)
                                (keyword? v) (f v)
                                :else v))
                            (vals m)))
      (keyword? m) (f m)
      :else m)))

(def all-keys->es-format
  (map-all-keys kw->es-format))

(def all-keys->kw
  (map-all-keys es-format->kw))

(def string-stored-not_analyzed
  {:type "string" 
   :store "yes"
   :index "not_analyzed"})

(defn build-index
  [conn]
  (let [mapping-types {doc-type {:properties 
                                 (all-keys->es-format
                                  {::ms/id string-stored-not_analyzed
                                   ::ms/type string-stored-not_analyzed
                                   ::ms/name {:type "string"
                                              :store "yes"}
                                   ::ss/id string-stored-not_analyzed
                                   ::ms/provenance {:type "nested"
                                                    :properties {::ms/source string-stored-not_analyzed
                                                                 :kixi.user/id string-stored-not_analyzed
                                                                 ::ms/pieces-count {:type "integer"}
                                                                 ::ms/parent-id string-stored-not_analyzed}}
                                   ::ms/segmentation {:type "nested"
                                                      :properties {::seg/type string-stored-not_analyzed
                                                                   ::seg/line-count {:type "integer"}
                                                                   ::seg/value string-stored-not_analyzed}}
                                   ::ms/sharing {:type "nested"
                                                 :properties (zipmap ms/activities
                                                                     (repeat string-stored-not_analyzed))}})}}]
    (esi/create conn index-name {:mappings mapping-types
                                 :settings {}})))

(defn ensure-index
  [conn]
  (when-not (esi/exists? conn index-name)
    (build-index conn)))

(defn- get-document
  [conn id]
  (esd/get conn
           index-name 
           doc-type 
           id
           {:preference "_primary"}))

(s/fdef update-metadata-processor
        :args (s/cat :conn #(instance? clojurewerkz.elastisch.rest.Connection %)
                     :update-req ::cs/file-metadata-updated))

(defmulti update-metadata-processor
  (fn [conn update-event]
    (::cs/file-metadata-update-type update-event)))

(def apply-attempts 10)

(defn version-conflict
  [resp]
  (some
   #(= "version_conflict_engine_exception"
       (:type %))
   ((comp :root_cause :error) resp)))

(defn error?
  [resp]
  (:error resp))

(defn- apply-func
  ([conn id f]
   (apply-func conn id f apply-attempts))
  ([conn id f tries]
   (let [curr (get-document conn id)]
     (let [resp (esd/put conn
                         index-name
                         doc-type
                         id
                         (f curr) 
                         (merge put-opts
                                (when (:_version curr)
                                  {:version (:_version curr)})))]
       (if (and (version-conflict resp)
                (pos? tries))
         (recur conn id f (dec tries))
         resp)))))

(defn- merge-metadata
  [conn id update]
  (let [r (apply-func
           conn
           id   
           (fn [curr]
             (merge-with merge
                         (:_source curr)
                         (all-keys->es-format update))))]
    (if (error? r)
      (error "Unable to merge File Metadata for id: " id ". Trying to merge: " update ". Response: " r)
      r)))

(defn- cons-metadata
  [conn id k element]
  (let [r (apply-func
           conn
           id   
           (fn [curr]
             (update (:_source curr) (kw->es-format k) 
                     #(cons (all-keys->es-format element) %))))]
    (if (error? r)
      (error "Unable to cons File Metadata for id: " id ". Trying to update: " k ". Response: " r)
      r)))

(defmethod update-metadata-processor ::cs/file-metadata-created
  [conn update-event]
  (let [metadata (::ms/file-metadata update-event)]
    (info "Update: " metadata)
    (merge-metadata
     conn
     (::ms/id metadata)
     metadata)))

(defmethod update-metadata-processor ::cs/file-metadata-structural-validation-checked
  [conn update-event]
  (info "Update: " update-event)
  (merge-metadata
   conn
   (::ms/id update-event)
   (select-keys update-event                
                [::ms/structural-validation])))

(defmethod update-metadata-processor ::cs/file-metadata-segmentation-add
  [conn update-event]
  (info "Update: " update-event)
  (cons-metadata
   conn
   (::ms/id update-event)
   ::ms/segmentations 
   (::ms/segmentation update-event)))

(defn response-event
  [r]
  nil)

(defrecord ElasticSearch
    [communications host port conn]
    MetaDataStore
    (authorised
      [this action id user-groups]
      (when-let [sharing (-> (get-document conn id)
                             :_source
                             (get (keyword (kw->es-format ::ms/sharing)))
                             all-keys->kw)]
        (not-empty (clojure.set/intersection (set (get sharing action))
                                             (set user-groups)))))
    (exists [this id]
      (esd/present? conn index-name doc-type id))
    (fetch [this id]
      (-> (get-document conn id)
          :_source
          all-keys->kw))
    (query [this criteria])

    component/Lifecycle
    (start [component]
      (if-not conn
        (let [connection (esr/connect (str "http://" host ":" port)
                                      {:connection-manager (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 10})})]
          (info "Starting ElasticSearch Metadata Store")
          (ensure-index connection)
          (c/attach-event-handler! communications
                                   :kixi.datastore/metadatastore
                                   :kixi.datastore/file-metadata-updated
                                   "1.0.0"
                                   (comp response-event (partial update-metadata-processor connection) :kixi.comms.event/payload))      
          (assoc component :conn connection))
        component))
    (stop [component]
      (if conn
        (do (info "Destroying ElasticSearch Metadata Store")
            (dissoc component :conn))
        component)))


