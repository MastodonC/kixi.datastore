(ns kixi.datastore.time
  (:require [clj-time.core :as t]
            [clj-time.format :as tf]))

(def format :basic-date-time)
(def date-format :basic-date)

(def es-format (clojure.string/replace (name format) "-" "_"))

(def formatter
  (tf/formatters format))

(def date-formatter
  (tf/formatters date-format))

(defn to-str
  [dt]
  (tf/unparse
   formatter
   dt))

(defn from-str
  [s]
  (tf/parse
   formatter
   s))

(defn timestamp
  []
  (tf/unparse
   formatter
   (t/now)))

(defn date
  []
  (tf/unparse
   date-formatter
   (t/now)))

(def now t/now)

(defn minutes-from-now
  [mins]
  (-> mins t/minutes t/from-now))

(defn midnight-timestamp?
  [x]
  (and (instance? org.joda.time.DateTime x)
       (zero? (t/hour x))
       (zero? (t/minute x))
       (zero? (t/milli x))))
