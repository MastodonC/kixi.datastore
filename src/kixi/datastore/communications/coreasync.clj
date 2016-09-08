(ns kixi.datastore.communications.coreasync
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :as async]
            [kixi.datastore.communications :refer [Communications]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn chan-mult
  [size]
  (let [c (async/chan size)]
    {:in c
     :out (async/mult c)}))

(defn tap-channel-with
  [cm selector processor put-result]
  (let [tapper (async/chan 5)]
    (async/go-loop [msg (async/<! tapper)]
      (when msg
        (try
          (when (selector msg)
            (async/go
              (let [result (processor msg)]
                (when put-result
                  (async/>! (:in cm)
                            result)))))
          (catch Exception e
            (error e (str "Exception while processing: " msg))))
        (recur (async/<! tapper))))
    (async/tap (:out cm)
               tapper)
    tapper))

(defrecord CoreAsync
    [buffer-size msg-chan processors-atom]
    Communications
    (submit-metadata [this metadata]
      (async/>!! (:in msg-chan)
                 metadata))
    (attach-pipeline-processor [this selector processor]
      (swap! processors-atom 
             assoc processor
             (tap-channel-with msg-chan selector processor true)))
    (attach-sink-processor [this selector processor]
      (swap! processors-atom 
             assoc processor
             (tap-channel-with msg-chan selector processor false)))
    (detach-processor [this processor]
      (when-let [detach-chan (get @processors-atom processor)]
        (async/untap (:out msg-chan)
                     detach-chan)
        (async/close! detach-chan)
        (swap! processors-atom
               dissoc processor)))

    component/Lifecycle
    (start [component]
      (if-not msg-chan
        (do
          (info "Starting CoreAsync Communications")
          (-> component
              (assoc :msg-chan (chan-mult buffer-size))
              (assoc :processors-atom (atom {}))))
        component))
    (stop [component]
      (if msg-chan
        (do       
          (info "Destroying CoreAsync Communications")
          (async/go
            (async/close! (:in msg-chan)))
          (async/untap-all (:out msg-chan))
          (doseq [attached (vals @processors-atom)]
            (async/close! attached))
          (-> component
              (dissoc msg-chan)
              (dissoc processors-atom)))
        component)))
