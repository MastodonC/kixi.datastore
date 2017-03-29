(ns kixi.datastore.cloudwatch
  (:require [amazonica.aws.cloudwatch :as cloudwatch]
            [taoensso.timbre :as log]))

(def default-threshold-percentage 0.9)
(def default-alarm-period 60)
(def default-evaluation-periods 1)

(defn safe-name
  [s]
  (cond
    (string? s) s
    (keyword? s) (name s)
    :else (str s)))

(defn put-dynamo-table-alarm
  [{:keys [metric table-name sns
           region description threshold
           alarm-period evaluation-periods]
    :or {evaluation-periods default-evaluation-periods}:as params}]
  (cloudwatch/put-metric-alarm {:endpoint region}
                               :alarm-name (str metric "-" (safe-name table-name))
                               :alarm-description description
                               :namespace "AWS/DynamoDB"
                               :metric-name metric
                               :dimensions [{:name "TableName" :value table-name}]
                               :statistic "Sum"
                               :threshold threshold
                               :comparison-operator "GreaterThanOrEqualToThreshold"
                               :period alarm-period
                               :evaluation-periods evaluation-periods
                               :alarm-actions [sns]))

(defn read-dynamo-alarm
  [{:keys [table-name] :as opts}]
  (put-dynamo-table-alarm (merge
                           opts
                           {:metric "ConsumedReadCapacityUnits"
                            :description (str "Alarm: read capacity almost at provisioned read capacity for " table-name)})))

(defn write-dynamo-alarm
  [{:keys [table-name] :as opts}]
  (put-dynamo-table-alarm (merge
                           opts
                           {:metric "ConsumedWriteCapacityUnits"
                            :description (str "Alarm: write capacity almost at provisioned write capacity for " table-name)})))

(defn table-dynamo-alarms
  [table-name {:keys [read-provision write-provision
                      threshold-percentage alarm-period]
               :or {threshold-percentage default-threshold-percentage
                    alarm-period default-alarm-period} :as opts}]
  (let [read-threshold (int (Math/ceil (* threshold-percentage alarm-period read-provision)))
        write-threshold (int (Math/ceil (* threshold-percentage alarm-period write-provision)))]
    (read-dynamo-alarm (assoc opts
                              :threshold read-threshold
                              :alarm-period alarm-period
                              :table-name table-name))
    (write-dynamo-alarm (assoc opts
                               :threshold write-threshold
                               :alarm-period alarm-period
                               :table-name table-name))))
