(ns kixi.datastore.logging
  (:require [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as log]))

(def logback-timestamp-opts
  {:pattern  "yyyy-MM-dd HH:mm:ss,SSS"
   :locale   :jvm-default
   :timezone :utc})

(def upper-name 
  (memoize 
   (fn [level]
     (str/upper-case (name level)))))

(defn output-fn
  "Default (fn [data]) -> string output fn.
  Use`(partial default-output-fn <opts-map>)` to modify default opts."
  ([data] (output-fn nil data))
  ([opts data] ; For partials
   (let [{:keys [surpress-stacktrace? stack-fonts]} opts
         {:keys [level ?err #_vargs msg_ ?ns-str hostname_
                 timestamp_ ?line]} data]
     (str
      (force timestamp_)  " "
      (upper-name level)  " "
      "[" (or ?ns-str "?") ":" (or ?line "?") "] - "
      (force msg_)
      (when-not surpress-stacktrace?
        (when-let [err ?err]
          (str "\n" (log/stacktrace err opts))))))))

(defn log-metrics
  [meter-mark!]
  (fn [data]
    (meter-mark! ["log" (name (:level data)) nil])
    data))

(defrecord Log
    [level ns-blacklist metrics]
    component/Lifecycle
    (start [component]
      (when-not (:full-config component)
        (let [full-config {:level level
                           :ns-blacklist ns-blacklist
                           :timestamp-opts logback-timestamp-opts ; iso8601 timestamps
                           :output-fn (partial output-fn {:stacktrace-fonts {}})
                           :middleware [(log-metrics (:meter-mark metrics))]}]
          (log/merge-config! full-config)
          (log/handle-uncaught-jvm-exceptions! 
           (fn [throwable ^Thread thread]
             (log/error throwable (str "Unhandled exception on " (.getName thread)))))
          (assoc component :full-config full-config))))
    (stop [component]
      (when (:full-config component)
        (dissoc component :full-config))))
