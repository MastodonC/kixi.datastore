(ns kixi.datastore.schemastore.inmemory
  (:require [clojure.spec :as s]
            [clojure.data :as data]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [kixi.datastore.schemastore :refer [SchemaStore] :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.comms :as c]
            [kixi.datastore.transit :as t]
            [kixi.datastore.time :as time]
            [taoensso.timbre :as timbre :refer [error info infof debug fatal]]))

(defn persist-new-schema
  [data schema]
  (let [id (::ss/id schema)
        schema' (assoc schema ::ss/timestamp (time/timestamp))]
    (if (s/valid? ::ss/stored-schema schema')
      (swap! data (fn [d] (assoc d id schema')))
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

(defn read-spec
  [data id]
  (fetch-with-sub-spec data {::ss/id id}))

(defn response-event
  [r]
  nil)

(defrecord InMemory
    [data communications]
    SchemaStore
    (authorised
      [_ action id user-groups]
      (when-let [meta (get @data id)]
        (not-empty (clojure.set/intersection (set (get-in meta [::ss/sharing action]))
                                             (set user-groups)))))
    (exists [_ id]
      (get @data id))
    (fetch-with [_ sub-spec]
      (fetch-with-sub-spec data sub-spec))
    (retrieve [_ id]
      (read-spec data id))
    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Schema Store")
          (c/attach-event-handler! communications
                                   :kixi.datastore/schemastore
                                   :kixi.datastore/schema-created
                                   "1.0.0"
                                   (comp response-event (partial persist-new-schema new-data) :kixi.comms.event/payload))
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Schema Store")
            (reset! data {})
            (dissoc component :data))
        component)))
