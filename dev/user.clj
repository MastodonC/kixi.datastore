(ns user
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.system :as system]
            [kixi.datastore.application :as app]
            [environ.core :refer [env]]))

(defn start
  ([]
   (start {} nil))
  ([overrides component-subset]
   (start "config.edn" "local"
          {:overrides overrides
           :component-subset component-subset}))
  ([config-location profile options]
   (let [{:keys [overrides component-subset]} options]
     (when-not @app/system
       (reset! app/profile (keyword (env :system-profile profile)))
       (reset! app/config-location config-location)
       (try
         (println "Starting system" profile)
         (->> (system/new-system config-location (keyword (env :system-profile profile)))
              (#(merge % overrides))
              (#(if component-subset
                  (select-keys % component-subset)
                  %))
              component/start-system
              (reset! app/system))
         (catch Exception e
           (reset! app/system (:system (ex-data e)))
           (throw e)))))))

(defn stop
  []
  (when @app/system
    (println "Stopping system")
    (component/stop-system @app/system)
    (reset! app/system nil)))

(defn restart
  []
  (stop)
  (start))
