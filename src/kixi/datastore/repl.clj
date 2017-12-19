(ns kixi.datastore.repl
  (:require [com.stuartsierra.component :as component]
            [cider.nrepl :refer (cider-nrepl-handler)]
            [clojure.tools.nrepl.server :as nrepl-server]
            [taoensso.timbre :as log]))

(defrecord ReplServer
    [port repl-server]
  component/Lifecycle
  (start [this]
    (if-not (:repl-server this)
      (do
        (log/info "Starting REPL server on" port)
        (assoc this :repl-server
               (nrepl-server/start-server :handler cider-nrepl-handler :bind "0.0.0.0" :port port)))
      this))
  (stop [this]
    (if (:repl-server this)
      (do (log/info "Stopping REPL server")
          (nrepl-server/stop-server (:repl-server this))
          (dissoc this :repl-server))
      this)))
