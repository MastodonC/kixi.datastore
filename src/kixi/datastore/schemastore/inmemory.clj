(ns kixi.datastore.schemastore.inmemory
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [kixi.datastore.schemastore :refer [SchemaStore]]
            [kixi.datastore.communications
             :refer [attach-sink-processor]]
            [kixi.datastore.transit :as t]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(s/def ::spec-name 
  keyword?)

(s/def ::predicate symbol?)

(s/def ::spec-definition
  (s/or 
   :predicate ::predicate
   ::expression (s/cat :conformer symbol?
                       :args (s/* (constantly true)))))

(s/fdef persist-new-schema
        :args (s/cat :spec-name ::spec-name
                     :spec-definition ::spec-definition))

(defn persist-new-schema
  [data spec-name spec-definition]
  (swap! data 
         assoc
         spec-name
         (t/clj-form->json-str spec-definition)))

(defn read-definition
  [data name]
  (some->> name
           (get @data)
           t/json-str->clj-form))

(defn read-spec
  [data name]
  (let [definition (read-definition data name)
        evald (eval definition)]
    (if (map? evald)
      (s/cat-impl (keys evald)
                     (map read-spec (vals evald))
                     '())
      (s/spec evald))))

(defrecord InMemory
    [data communications]
    SchemaStore
    (fetch-definition [_ name]
      (read-definition data name))
    (fetch-spec [_ name]
      (read-spec data name))
    (persist [_ spec-name spec-definition]
      (persist-new-schema
       data spec-name spec-definition))
    component/Lifecycle
    (start [component]
      (if-not data
        (let [new-data (atom {})]
          (info "Starting InMemory Schema Store")
          (attach-sink-processor communications
                                 (partial instance? ::spec-definition) ;this needs a good upgrade
                                 (partial persist-new-schema new-data) ;this won't work!
                                 )
          (assoc component :data new-data))
        component))
    (stop [component]
      (if data
        (do (info "Destroying InMemory Schema Store")
            (reset! data {})
            (dissoc component :data))
        component)))
