(ns kixi.datastore.metrics
  (:require [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [kixi.metrics.name-safety :refer [safe-name]]
            [kixi.metrics.reporters.json-console :as reporter]
            [metrics
             [core :refer [new-registry]]
             [histograms :refer [histogram update!]]
             [meters :refer [mark! meter]]]
            [metrics.jvm.core :as jvm]
            [metrics.ring.expose :as expose]
            [taoensso.timbre :as log]))

(def upper-name 
  (memoize 
   (fn [level]
     (str/upper-case (name level)))))

(def uri-method-status->metric-name
  (memoize 
   (fn [uri method status]
     (let [name (-> (str "." (upper-name method))
                    safe-name
                    (str "." status))]
       ["resources" (safe-name uri) name]))))

(defn request-ctx->metric-name
  [ctx]
  (uri-method-status->metric-name
   (get-in ctx [:request :uri])
   (:method ctx)
   (get-in ctx [:response :status])))

(defn now
  []
  (System/currentTimeMillis))

(defn append-rates
  [metric-name]
  (update metric-name 2
          #(str % ".rates")))

(defn insert-time
  "Interceptor that stores the current time in the context under [:metrics ky]"
  [ky]
  (fn [ctx]
    (assoc-in ctx [:metrics ky] (now))))

(defn record-metrics
  "Interceptor that records the time since [:metrics ky]"
  [registry ky]
  (fn [ctx]
    (let [metric-name (request-ctx->metric-name ctx)
          met (meter registry (append-rates metric-name))]
      (when-let [start-time (get-in ctx [:metrics ky])]
      ;On exception this won't get the modified context, i.e. the one with start time in it. This is due to the manifold chain and catch mechinism used.
        (update! (histogram registry metric-name) 
                 (- (now) start-time)))
      (mark! met))
    ctx))

(defn expose-metrics-resource
  [registry]
  {:methods
   {:get
    {:produces "application/json"
     :response #(expose/render-metrics registry)}}})

(defn meter-mark!
  [registry]
  (fn [meter-name]
    (let [met (meter registry (mapv safe-name meter-name))]
      (mark! met))))

(defrecord Metrics
    [json-reporter registry]  
    component/Lifecycle
    (start [component]
      (if-not registry
        (let [with-reg (update component :registry #(or %
                                                        (let [reg (new-registry)]
                                                          (jvm/instrument-jvm reg)
                                                          reg)))
              reg (:registry with-reg)]
          (-> with-reg
              (update :json-reporter-inst #(or %
                                               (let [reporter (reporter/reporter reg {})]
                                                 (log/info "Starting JSON Metrics Reporter")
                                                 (reporter/start reporter (:seconds json-reporter))
                                                 reporter)))
              (update :meter-mark #(or %
                                       (meter-mark! reg)))
              (update :insert-time-in-ctx #(or %
                                               (insert-time :resource)))
              (update :expose-metrics-resource #(or %
                                                    (expose-metrics-resource reg)))
              (update :record-ctx-metrics #(or %
                                               (record-metrics reg :resource)))))
        component))
    (stop [component]
      (if registry
        (-> component
            (update :json-reporter-inst #(when %
                                           (log/info "Stopping JSON Reporting")
                                           (reporter/stop %)
                                           nil))
            (dissoc :meter-mark)
            (dissoc :insert-time-in-ctx)
            (dissoc :record-ctx-metrics)
            (update :registry (fn [^com.codahale.metrics.MetricRegistry mr] 
                                (when mr
                                  (log/info "Destorying metrics registry")
                                  (.removeMatching mr (com.codahale.metrics.MetricFilter/ALL))
                                  nil))))
        component)))

