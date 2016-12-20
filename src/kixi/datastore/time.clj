(ns kixi.datastore.time
  (:require [clj-time.core :as t]
            [clj-time.format :as tf]))

(def format :basic-date-time)

(def es-format (clojure.string/replace (name format) "-" "_"))

(def formatter
  (tf/formatters format))

(defn timestamp
  []
  (tf/unparse
   formatter
   (t/now)))

(defn minutes-from-now
  [mins]
  (-> mins t/minutes t/from-now))
