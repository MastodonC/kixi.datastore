(ns kixi.datastore.schemastore.inmemory
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [kixi.datastore.schemastore :refer [SchemaStore] :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [kixi.datastore.transit :as t]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn persist-new-schema
  [data create-request]
  (swap! data 
         assoc
         (::ss/name create-request)
         (t/clj-form->json-str (::ss/definition create-request))))

(defn read-definition
  [data name]
  (some->> name
           (get @data)
           t/json-str->clj-form))

(defn read-spec
  [data name]
  (let [definition (read-definition data name)
        evald (eval definition)]
    (s/spec evald))) ;; Need to reach into the map for :ps values (?) and load them from the store

(defrecord InMemory
    [data communications]
    SchemaStore
    (exists [_ name]
      (get @data name))
    (fetch-definition [_ name]
      (read-definition data name))
    (fetch-spec [_ name]
      (read-spec data name))
    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Schema Store")
          (attach-sink-processor communications
                                 #(s/valid? ::ss/create-request %)
                                 (partial persist-new-schema new-data))
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Schema Store")
            (reset! data {})
            (dissoc component :data))
        component)))
