(ns kixi.datastore.cloudwatch
  (:require [amazonica.aws.cloudwatch :as cloudwatch]
            [taoensso.timbre :as log]))

(def default-threshold 0.9)
(def default-alarm-period 60)
(def default-evaluation-period 1)

(defn put-dynamo-table-alarm
  [{:keys [metric table-name sns
           region description threshold
           alarm-period evaluation-period]
    :or {threshold default-threshold
         alarm-period default-alarm-period
         evaluation-period default-evaluation-period}:as params}]
  (cloudwatch/put-metric-alarm {:endpoint region}
                               :alarm-name (str metric "-" (name table-name))
                               :alarm-description description
                               :namespace "AWS/DynamoDB"
                               :metric-name metric
                               ;;                   :dimensions ["Tablename" table-name] ;;(str "name=TableName,value=" table-name)
                               :statistic "Sum"
                               :threshold threshold
                               :comparison-operator "GreaterThanOrEqualToThreshold"
                               :period alarm-period
                               :evaluation-periods 1
                               :alarm-actions [sns]))

(defn read-dynamo-alarm
  [opts]
  (put-dynamo-table-alarm (merge
                           opts
                           {:metric "ConsumedReadCapacityUnits"
                            :description (str "Alarm: read capacity almost at provisioned read capacity for " table-name)})))

(defn write-dynamo-alarm
  [opts]
  (put-dynamo-table-alarm (merge
                           opts
                           {:metric "ConsumedWriteCapacityUnits"
                            :description (str "Alarm: write capacity almost at provisioned write capacity for " table-name)})))

(defn table-dynamo-alarms
  [table-name opts]
  (read-dynamo-alarm opts)
  (write-dynamo-alarm opts))
