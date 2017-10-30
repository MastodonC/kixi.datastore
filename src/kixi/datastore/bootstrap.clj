(ns kixi.datastore.bootstrap
  (:require [kixi.datastore.system]
            [kixi.datastore.application :as application]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]
            [signal.handler :refer [with-handler]])
  (:gen-class))

(defn -main
  [& args]
  (let [config-profile (keyword (first args))
        config-location (or (second args) "config.edn")
        _ (reset! application/profile config-profile)
        _ (reset! application/config-location config-location)
        system (kixi.datastore.system/new-system config-location config-profile)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(do (component/stop-system system)
                   (reset! application/system nil)
                   (reset! application/profile nil))))
    (try
      (reset! application/system
              (component/start-system system))
      (.. (Thread/currentThread) join)
      (catch Throwable t
        (log/error t "Top level exception caught")))
    (with-handler :term
      (log/info "SIGTERM was caught: shutting down...")
      (component/stop system))))
