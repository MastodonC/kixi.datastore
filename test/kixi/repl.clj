(ns kixi.repl
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.system :as system]))

(defonce system (atom nil))

(defn start
  []
  (when-not @system
    (try
      (prn "Starting system")
      (->> (system/new-system :local)
           component/start-system
           (reset! system))
      (catch Exception e
        (reset! system (:system (ex-data e)))
        (throw e)))))

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
