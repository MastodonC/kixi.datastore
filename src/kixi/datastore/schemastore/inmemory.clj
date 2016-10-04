(ns kixi.datastore.schemastore.inmemory
  (:require [clojure.spec :as s]
            [clojure.data :as data]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [kixi.datastore.schemastore :refer [SchemaStore] :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [kixi.datastore.transit :as t]
            [taoensso.timbre :as timbre :refer [error info infof debug]]))

(defn persist-new-schema
  [data schema]
  (let [id (::ss/id schema)
        schema' (assoc schema ::ss/timestamp (str (java.util.Date.)))]
    (if (s/valid? ::ss/stored-schema schema')
      (swap! data (fn [d] (assoc d id schema')))
      (error "Tried to persist schema but it was invalid:" schema' (s/explain-data ::ss/stored-schema schema')))))

(defn sub-map
  [f s]
  (-> (data/diff f s)
      first
      not))

(defn fetch-with-sub-spec
  [data sub-spec]
  (some->> (vals @data)
           (some #(when (sub-map sub-spec %) %))))

#_(defn resolve-spec
    [spec-sym get-spec-fn]
    (let [[initial & forms] spec-sym]
      (when (= initial 'clojure.spec/cat)
        (doseq [f (->> forms
                       (partition 2)
                       (map second))]
          (let [inner-spec (get-spec-fn f)]
            (s/def-impl f inner-spec (eval inner-spec)))))
      (s/spec (eval spec-sym))))

(defn read-spec
  [data id]
  (fetch-with-sub-spec data {::ss/id id}))

(defrecord InMemory
    [data communications]
  SchemaStore
  (exists [_ id]
    (get @data id))
  (fetch-with [_ sub-spec]
    (fetch-with-sub-spec data sub-spec))
  (fetch-spec [_ id]
    (read-spec data id))
  component/Lifecycle
  (start [component]
    (if-not data
      (let [new-data (atom {})]
        (info "Starting InMemory Schema Store")
        (attach-sink-processor communications
                               #(s/valid? ::ss/create-schema-request %)
                               (partial persist-new-schema new-data))
        (assoc component :data new-data))
      component))
  (stop [component]
    (if data
      (do (info "Destroying InMemory Schema Store")
          (reset! data {})
          (dissoc component :data))
      component)))
