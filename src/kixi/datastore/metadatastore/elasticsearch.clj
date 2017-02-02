(ns kixi.datastore.metadatastore.elasticsearch
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore
             [communication-specs :as cs]
             [elasticsearch :as es :refer [migrate]]
             [metadatastore :as ms :refer [MetaDataStore]]]
            [taoensso.timbre :as timbre :refer [info]]))

(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

(s/fdef update-metadata-processor
        :args (s/cat :conn #(instance? clojurewerkz.elastisch.rest.Connection %)
                     :update-req ::cs/file-metadata-updated))

(def merge-data
  (partial es/merge-data index-name doc-type))

(def update-in-data
  (partial es/update-in-data index-name doc-type))

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
  (update-in-data 
   conn
   (::ms/id update-event)
   conj
   [::ms/segmentations]
   (::ms/segmentation update-event)))

(defmethod update-metadata-processor ::cs/file-metadata-sharing-updated
  [conn update-event]
  (info "Update: " update-event)
  (let [update-fn (case (::ms/sharing-update update-event)
                    ::ms/sharing-conj conj
                    ::ms/sharing-disj disj)]
    (update-in-data conn
                    (::ms/id update-event)
                    update-fn
                    [::ms/sharing (::ms/activity update-event)]
                    (:kixi.group/id update-event))))

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
    [communications host port discover migrators-dir conn]
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
    (query [this criteria from-index count sort-by sort-order]
      (search conn
              (query-criteria->es-query criteria)
              from-index count
              sort-by sort-order))

    component/Lifecycle
    (start [component]
      (if-not conn
        (let [[host port] (if discover (es/discover-executor discover) [host port])
              connection (es/connect host port)
              joplin-conf {:migrators {:migrator "joplin/kixi/datastore/metadatastore/migrators/"}
                           :databases {:es {:type :es :host host :port port :migration-index "metadatastore-migrations"}}
                           :environments {:env [{:db :es :migrator :migrator}]}}]
          (info "Starting File Metadata ElasticSearch Store")
          (migrate :env joplin-conf)
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
