(ns kixi.datastore.time
  (:require [clj-time.core :as t]
            [clj-time.format :as tf]))

(defn timestamp
  []
  (tf/unparse
   (tf/formatters :basic-date-time)
   (t/now)))
