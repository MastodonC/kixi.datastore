(ns kixi.datastore.schemastore.conformers
  (:require [clojure.core :exclude [integer? double? set?]]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.test.check.generators :as tgen]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [kixi.datastore.time :as time]))

(defn double->int
  "1.0 will convert to 1; anything else will be rejected"
  [d]
  (let [int-val (int d)]
    (when (== int-val d)
      int-val)))

(defn str->double
  "Strings converted to doubles"
  [^String s]
  (try
    (Double/valueOf (str s))
    (catch Exception e
      :clojure.spec/invalid)))

(defn str-double->int
  "1, 1. or 1.0...0 will convert to 1"
  [^String s]
  (try
    (double->int (str->double s))
    (catch Exception e
      nil)))

(defn str->int
  [^String s]
  (try
    (Integer/valueOf s)
    (catch NumberFormatException e
      (or
        (str-double->int s)     
        :clojure.spec/invalid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integer

(defn -integer?
  [x]
  (cond (string? x) (str->int x)
        (clojure.core/integer? x) x
        (and (clojure.core/double? x)
             (double->int x))     (double->int x)
        :else :clojure.spec/invalid))

(def integer? (s/conformer -integer?))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Double

(defn -double?
  [x]
  (cond
    (clojure.core/double? x) x
    (clojure.core/integer? x) (double x)
    (string? x) (str->double x)
    :else :clojure.spec/invalid))

(def double? (s/conformer -double?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integer Range

(defn -integer-range?
  [min max]
  (let [rcheck (fn [v] (if (>= max v min)
                         v
                         :clojure.spec/invalid))]
    (fn [x]
      (let [r (-integer? x)]
        (if (= r :clojure.spec/invalid)
          r
          (rcheck r))))))

(defn integer-range?
  [min max]
  (when (or (not (int? min))
            (not (int? max)))
    (throw (IllegalArgumentException. "Both min and max must be integers")))
  (s/conformer (-integer-range? min max)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Double Range

(defn -double-range?
  [min max]
  (let [rcheck (fn [v] (if (>= max v min)
                         v
                         :clojure.spec/invalid))]
    (fn [x]
      (let [r (-double? x)]
        (if (= r :clojure.spec/invalid)
          r
          (rcheck r))))))

(defn double-range?
  [min max]
  (when (or (not (clojure.core/double? min))
            (not (clojure.core/double? max)))
    (throw (IllegalArgumentException. "Both min and max must be doubles")))
  (s/conformer (-double-range? min max)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Set

(defn -set?
  [sargs]
  (fn [x]
    (if (sargs x) x :clojure.spec/invalid)))

(defn set?
  [& sargs]
  (s/conformer (-set? (set sargs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Regex

(defn -regex?
  [rs]
  (fn [x]
    (if (and (string? x) (re-find rs x)) 
      x 
      :clojure.spec/invalid)))

(defn regex?
  [rs]
  (let [msg (str rs " is not a valid regex.")]
    (if (or (= (type rs) java.util.regex.Pattern)
            (string? rs))
      (try
        (s/conformer (-regex? (re-pattern rs)))
        (catch java.util.regex.PatternSyntaxException _
          (throw (IllegalArgumentException. msg))))
      (throw (IllegalArgumentException. msg)))))

(defn -bool?
  [x]
  (cond
    (boolean? x) x
    (string? x) (case x
                  ("true" "TRUE" "t" "T") true
                  ("false" "FALSE" "f" "F") false
                  :clojure.spec/invalid)
    :else :clojure.spec/invalid))

(def bool? 
  (s/with-gen (s/conformer -bool?)
    (constantly (gen/boolean))))

(defn -string?
  [x]
  (cond
    (string? x) x
    :else (str x)))

(def time-parser   
  (partial tf/parse time/formatter))

(def date-parser   
  (partial tf/parse time/date-formatter))

(defn timestamp?
  [x]
  (if (instance? org.joda.time.DateTime x)
    x
    (try
      (if (string? x)
        (time-parser x)
        :clojure.spec/invalid)
      (catch IllegalArgumentException e
        :clojure.spec/invalid))))

(def timestamp
  (s/with-gen
    (s/conformer timestamp?)
    #(gen/return (t/now))))

(defn date?
  [x]
  (if (or (instance? org.joda.time.DateMidnight x)
          (time/midnight-timestamp? x))
    x
    (try
      (if (string? x)
        (date-parser x)
        :clojure.spec/invalid)
      (catch IllegalArgumentException e
        :clojure.spec/invalid))))

(def date
  (s/with-gen
    (s/conformer date?)
    #(gen/return (t/today-at-midnight))))

(def uuid?
  (-regex? #"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))

(def uuid 
  (s/with-gen 
    (s/conformer uuid?)
    #(tgen/no-shrink (gen/fmap str (gen/uuid)))))

(def anything 
  (s/with-gen (constantly true)
    #(gen/any)))

(defn ns-keyword?
  [x]
  (cond
    (and (keyword? x)
         (namespace x)) x
    (string? x) (try 
                  (let [kw (apply keyword (clojure.string/split x #"/" 2))]
                    (if (namespace kw)
                      kw
                      :clojure.spec/invalid))
                  (catch Exception e
                    :clojure.spec/invalid))
    :else :clojure.spec/invalid))

(def ns-keyword
  (s/with-gen
    (s/conformer ns-keyword?)
    #(gen/such-that namespace (gen/keyword-ns))))

(defn -not-empty-string?
  [x]
  (and (string? x)
       (not-empty x)))

(def not-empty-string
  (s/with-gen 
    (s/conformer -not-empty-string?)
    #(gen/not-empty (s/gen string?))))
