(ns kixi.datastore.logging
  (:require [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]))

(defn log-metrics
  [meter-mark!]
  (fn [data]
    (meter-mark! ["log" (name (:level data)) nil])
    data))

(defrecord Log
    [level ns-blacklist metrics logstash-appender?
     full-config]
  component/Lifecycle
  (start [component]
    (if-not full-config
      (let [full-config {:middleware [(log-metrics (:meter-mark metrics))]}]
        (log/merge-config! full-config)
        (assoc component :full-config full-config))
      component))
  (stop [component]
    (if full-config
      (dissoc component :full-config)
      component)))
