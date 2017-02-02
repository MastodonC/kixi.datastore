(ns kixi.datastore.bootstrap
  (:require [kixi.datastore.system]
           [com.stuartsierra.component :as component])
  (:gen-class))

(defn -main 
  [& args]
  (let [config-profile (keyword (first args))
        system (kixi.datastore.system/new-system config-profile)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(component/stop-system system)))
    (try
      (component/start-system system)
      (.. (Thread/currentThread) join)
      (catch Throwable t
                 (log/error t "Top level exception caught")))))
