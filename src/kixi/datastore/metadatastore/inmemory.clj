(ns kixi.datastore.metadatastore.inmemory
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore
             :refer [MetaDataStore] :as ms]
            [kixi.comms :as c]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(s/fdef update-metadata-processor
        :args (s/cat :data #(instance? clojure.lang.IAtom %)
                     :update-req ::ms/file-metadata-updated))

(defmulti update-metadata-processor
  (fn [data update-event]
    (::ms/file-metadata-update-type update-event)))

(defmethod update-metadata-processor ::ms/file-metadata-created
  [data update-event]
  (let [metadata (::ms/file-metadata update-event)]
    (info "Update: " metadata)
    (swap! data
           #(update % (::ms/id metadata)
                    (fn [current-metadata]
                      (merge current-metadata
                             metadata))))))


(defmethod update-metadata-processor ::ms/file-metadata-structural-validation-checked
  [data update-event]
  (info "Update: " update-event)
  (swap! data
         #(update % (::ms/id update-event)
                  (fn [current-metadata]
                    (assoc (or current-metadata {})
                           ::ms/structural-validation (::ms/structural-validation update-event))))))

(defmethod update-metadata-processor ::ms/file-metadata-segmentation-add
  [data update-event]
  (info "Update: " update-event)
  (swap! data
         #(update % (::ms/id update-event)
                  (fn [current-metadata]
                    (update (or current-metadata {})
                            ::ms/segmentations (fn [segs] 
                                                 (cons (::ms/segmentation update-event) segs)))))))


(defn response-event
  [r]
  nil)

(defrecord InMemory
    [data communications]
    MetaDataStore
    (exists [this id]
      ((set
        (keys @data))
       id))
    (fetch [this id]
      (get @data id))
    (query [this criteria])

    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Metadata Store")
          (c/attach-event-handler! communications
                                   :metadatastore
                                   :kixi.datastore/file-metadata-updated
                                   "1.0.0"
                                   (comp response-event (partial update-metadata-processor new-data) :kixi.comms.event/payload))      
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Metadata Store")
            (reset! data {})
            (dissoc component :data))
        component)))
