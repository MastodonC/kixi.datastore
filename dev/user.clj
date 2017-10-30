(ns user
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.system :as system]
            [kixi.datastore.application :as app]
            [environ.core :refer [env]]))

(defn start
  ([]
   (start {} nil))
  ([overrides component-subset]
   (start "config.edn" "local" overrides component-subset))
  ([config-location profile overrides component-subset]
   (when-not @app/system
     (reset! app/profile (keyword (env :system-profile profile)))
     (reset! app/config-location config-location)
     (try
       (prn "Starting system")
       (->> (system/new-system config-location (keyword (env :system-profile profile)))
            (#(merge % overrides))
            (#(if component-subset
                (select-keys % component-subset)
                %))
            component/start-system
            (reset! app/system))
       (catch Exception e
         (reset! app/system (:system (ex-data e)))
         (throw e))))))

(defn stop
  []
  (when @app/system
    (prn "Stopping system")
    (component/stop-system @app/system)
    (reset! app/system nil)))

(defn restart
  []
  (stop)
  (start))
