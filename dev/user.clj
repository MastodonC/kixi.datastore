(ns user
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.system :as system]
            [environ.core :refer [env]]))

(defonce system (atom nil))

(defn start
  ([]
   (start {} nil))
  ([overrides component-subset]
   (start "local" overrides component-subset))
  ([profile overrides component-subset]
   (when-not @system
     (try
       (prn "Starting system")
       (->> (system/new-system (keyword (env :system-profile profile)))
            (#(merge % overrides))
            (#(if component-subset
                (select-keys % component-subset)
                %))
            component/start-system
            (reset! system))
       (catch Exception e
         (reset! system (:system (ex-data e)))
         (throw e))))))

(defn stop
  []
  (when @system
    (prn "Stopping system")
    (component/stop-system @system)
    (reset! system nil)))

(defn restart
  []
  (stop)
  (start))
