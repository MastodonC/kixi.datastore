(ns kixi.datastore.metrics
  (:require [clojure.string :as str]
            [com.stuartsierra.component :as component]
            [metrics
             [core :refer [new-registry]]
             [histograms :refer [histogram update!]]
             [meters :refer [mark! meter]]]
            [metrics.jvm.core :as jvm]
            [metrics.reporters.influxdb :as influxdb]
            [metrics.ring.expose :as expose]
            [taoensso.timbre :as log]))

(def upper-name 
  (memoize 
   (fn [level]
     (str/upper-case (name level)))))

;; Lifted all this from silcon-gorge/radix

(def replace-guid
  [#"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}" "GUID"])

(def replace-number
  [#"[0-9][0-9]+" "NUMBER"])

(def replace-territory
  [#"/[a-z]{2}/" "/TERRITORIES/"])

(defn- apply-regex
  [path [pattern replacement]]
  (str/replace path pattern replacement))

(defn- apply-aggregations
  [path aggregations]
  (reduce apply-regex path aggregations))

(defn clean-metric-name
  [name]
  (-> name
      (str/replace " " ".")
      (str/replace "./" ".")
      (str/replace "/" ".")
      (str/replace #"\p{Cntrl}" "")
      (str/replace #"^\.+" "")
      (str/replace #"\.+$" "")))

(def default-aggregations [replace-guid replace-number replace-territory])

(def uri-method-status->metric-name
  (memoize 
   (fn [uri method status]
     (let [name (-> (str "." (upper-name method))
                    (apply-aggregations default-aggregations)
                    clean-metric-name
                    (str "." status))]
       ["resources" (clean-metric-name uri) name]))))

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
    (let [met (meter registry meter-name)]
      (mark! met))))

(defrecord Metrics
    [influx-reporter registry]  
    component/Lifecycle
    (start [component]
      (if-not registry
        (let [with-reg (update component :registry #(or %
                                                        (let [reg (new-registry)]
                                                          (jvm/instrument-jvm reg)
                                                          reg)))
              reg (:registry with-reg)]
          (-> with-reg
              (update :reporter #(or %
                                     (let [reporter (influxdb/reporter reg 
                                                                       influx-reporter)]
                                       (log/info "Starting InfluxDb Metrics Reporter")
                                       (influxdb/start reporter (:seconds influx-reporter))
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
            (update :reporter #(when %
                                 (log/info "Stopping InfluxDb Reporting")
                                 (influxdb/stop %)
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

