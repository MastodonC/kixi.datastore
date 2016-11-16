(ns kixi.datastore.schemastore.elasticsearch
  (:require [clojure
             [data :as data]
             [spec :as s]]
            [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore
             [elasticsearch :as es :refer [ensure-index string-analyzed string-stored-not_analyzed]]
             [schemastore :as ss :refer [SchemaStore]]
             [time :as time]]
            [taoensso.timbre :as timbre :refer [error info]]))

(def index-name "kixi-datastore_schema-data")
(def doc-type "schema-data")

(def doc-def
  {::ss/id string-stored-not_analyzed
   ::ss/name string-analyzed
   ::ss/timestamp es/timestamp
   ::ss/schema {:type "nested"
                :properties {::ss/tag string-stored-not_analyzed
                             ::ss/type string-stored-not_analyzed
                             ::ss/id string-stored-not_analyzed
                             ::ss/min es/number
                             ::ss/max es/number
                             ::ss/pattern string-stored-not_analyzed
                             ::ss/elements string-stored-not_analyzed
                             ::ss/definition {:type "nested"
                                              :properties {::ss/type string-stored-not_analyzed
                                                           ::ss/id string-stored-not_analyzed
                                                           ::ss/min es/number
                                                           ::ss/max es/number
                                                           ::ss/pattern string-stored-not_analyzed
                                                           ::ss/elements string-stored-not_analyzed}}}}})

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

(defn inject-tag
  [def]
  (reduce
   (fn [acc [tag def]]
     (conj acc
           (assoc def
                  ::ss/tag tag)))
   []
   (partition 2 def)))

(defn inject-tags
  [schema]
  (if (get-in schema [::ss/schema ::ss/definition])
    (update-in schema 
               [::ss/schema ::ss/definition]
               inject-tag)
    schema))

(defn extract-tag
  [stored-def]
  (mapcat
   (fn [def]
     [(::ss/tag def)
      (dissoc def
              ::ss/tag)])
   stored-def))

(defn extract-tags
  [stored-schema]
  (if (get-in stored-schema [::ss/schema ::ss/definition])
    (update-in stored-schema
               [::ss/schema ::ss/definition]
               extract-tag)
    stored-schema))

(defn persist-new-schema
  [conn schema]  
  (let [id (::ss/id schema)
        schema' (assoc schema ::ss/timestamp (time/timestamp))]
    (if (s/valid? ::ss/stored-schema schema')
      (merge-data conn id (inject-tags schema'))
      (error "Tried to persist schema but it was invalid:" schema' (s/explain-data ::ss/stored-schema schema')))))                               ;should be at the command level

(defn sub-map
  [f s]
  (-> (data/diff f s)
      first
      not))

(defn fetch-with-sub-spec
  [data sub-spec]
  (some->> (vals @data)
           (some #(when (sub-map sub-spec %) %))))

(defn response-event
  [r]
  nil)

(defrecord ElasticSearch
    [communications host port conn]
    SchemaStore
    (authorised
      [_ action id user-groups]
      (when-let [sharing (get-document-key conn id ::ss/sharing)]
        (not-empty (clojure.set/intersection (set (get sharing action))
                                             (set user-groups)))))
    (exists [_ id]
      (present? conn id))
    (fetch-with [_ sub-spec]
      (prn "fetching: " sub-spec)
;      (fetch-with-sub-spec data sub-spec)
      )
    (retrieve [_ id]
      (extract-tags
       (get-document conn id)))
    component/Lifecycle
    (start [component]
      (if-not conn
        (let [connection (es/connect host port)]
          (info "Starting Schema ElasticSearch Store")
          (ensure-index index-name
                        doc-type 
                        doc-def
                        connection)
          (c/attach-event-handler! communications
                                   :kixi.datastore/schemastore
                                   :kixi.datastore/schema-created
                                   "1.0.0"
                                   (comp response-event (partial persist-new-schema connection) :kixi.comms.event/payload))      
          (assoc component :conn connection))
        component))
    (stop [component]
      (if conn
        (do (info "Destroying Schema ElasticSearch Store")
            (dissoc component :conn))
        component)))