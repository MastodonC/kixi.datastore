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
        _ (reset! application/profile config-profile)
        system (kixi.datastore.system/new-system config-profile)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(component/stop-system system)))
     (reset! application/system nil)
     (reset! application/profile nil)
    (try
      (component/start-system system)
      (reset! application/system system)
      (.. (Thread/currentThread) join)
      (catch Throwable t
        (log/error t "Top level exception caught")))
    (with-handler :term
      (log/info "SIGTERM was caught: shutting down...")
      (component/stop system))))
