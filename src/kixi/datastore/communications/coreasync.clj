(ns kixi.datastore.communications.coreasync
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [kixi.datastore.protocols :refer [Communications]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn chan-mult
  [size]
  (let [c (async/chan size)]
    {:in c
     :out (async/mult c)}))

(defn tap-channel-with
  [cm processor]
  (let [tapper (async/chan)]
    (async/go-loop [metadata (async/<! tapper)]
      (try
        (when metadata
          (processor metadata))
        (catch Exception e
          (error e (str "Exception while processing: " metadata))))
      (recur (async/<! tapper)))
    (async/tap (:out cm)
               tapper)))

(def channels [:new-metadata-chan :update-metadata-chan])

(defrecord CoreAsync
    [buffer-size new-metadata-chan update-metadata-chan]
    Communications
    (new-metadata [this meta-data]
      (async/>!! (:in new-metadata-chan)
                 meta-data))
    (attach-new-metadata-processor [this processor]
      (tap-channel-with new-metadata-chan processor))
    (update-metadata [this metadata-update]
      (async/>!! (:in update-metadata-chan)
                 metadata-update))
    (attach-update-metadata-processor [this processor]
      (tap-channel-with update-metadata-chan processor))

    component/Lifecycle
    (start [component]
      (info "Starting CoreAsync Communications")
      (merge component 
             (zipmap channels 
                     (repeatedly #(chan-mult buffer-size)))))
    (stop [component]
      (info "Destroying CoreAsync Communications")
      (doseq [cm (map #(get component %) channels)]
        (async/go
          (async/close! (:in cm)))
        (async/untap-all (:out cm)))
      (apply dissoc component channels)))
