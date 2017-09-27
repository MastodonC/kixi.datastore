(ns kixi.datastore.schemastore.conformers
  (:require [clojure.core :exclude [integer? double? set?]]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test.check.generators :as tgen]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [kixi.datastore.time :as time]))

(s/fdef alias
        :args (s/cat :alias simple-symbol? :ns simple-symbol?)
        :ret nil?)

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
      :clojure.spec.alpha/invalid)))

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
        :clojure.spec.alpha/invalid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integer

(defn -integer?
  [x]
  (cond (string? x) (str->int x)
        (clojure.core/integer? x) x
        (and (clojure.core/double? x)
             (double->int x))     (double->int x)
        :else :clojure.spec.alpha/invalid))

(def integer? (s/conformer -integer? identity))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Double

(defn -double?
  [x]
  (cond
    (clojure.core/double? x) x
    (clojure.core/integer? x) (double x)
    (string? x) (str->double x)
    :else :clojure.spec.alpha/invalid))

(def double? (s/conformer -double? identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integer Range

(defn -integer-range?
  [min max]
  (let [rcheck (fn [v] (if (>= max v min)
                         v
                         :clojure.spec.alpha/invalid))]
    (fn [x]
      (let [r (-integer? x)]
        (if (= r :clojure.spec.alpha/invalid)
          r
          (rcheck r))))))

(defn integer-range?
  [min max]
  (when (or (not (int? min))
            (not (int? max)))
    (throw (IllegalArgumentException. "Both min and max must be integers")))
  (s/conformer (-integer-range? min max) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Double Range

(defn -double-range?
  [min max]
  (let [rcheck (fn [v] (if (>= max v min)
                         v
                         :clojure.spec.alpha/invalid))]
    (fn [x]
      (let [r (-double? x)]
        (if (= r :clojure.spec.alpha/invalid)
          r
          (rcheck r))))))

(defn double-range?
  [min max]
  (when (or (not (clojure.core/double? min))
            (not (clojure.core/double? max)))
    (throw (IllegalArgumentException. "Both min and max must be doubles")))
  (s/conformer (-double-range? min max) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Set

(defn -set?
  [sargs]
  (fn [x]
    (if (sargs x) x :clojure.spec.alpha/invalid)))

(defn set?
  [& sargs]
  (s/conformer (-set? (set sargs)) identity))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Regex

(defn -regex?
  [rs]
  (fn [x]
    (if (and (string? x) (re-find rs x)) 
      x 
      :clojure.spec.alpha/invalid)))

(defn regex?
  [rs]
  (let [msg (str rs " is not a valid regex.")]
    (if (or (= (type rs) java.util.regex.Pattern)
            (string? rs))
      (try
        (s/conformer (-regex? (re-pattern rs)) identity)
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
                  :clojure.spec.alpha/invalid)
    :else :clojure.spec.alpha/invalid))

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

(def time-unparser
  (partial tf/unparse time/formatter))

(def date-parser   
  (partial tf/parse time/date-formatter))

(def date-unparser
  (partial tf/unparse time/date-formatter))

(defn timestamp?
  [x]
  (if (instance? org.joda.time.DateTime x)
    x
    (try
      (if (string? x)
        (time-parser x)
        :clojure.spec.alpha/invalid)
      (catch IllegalArgumentException e
        :clojure.spec.alpha/invalid))))

(def timestamp
  (s/with-gen
    (s/conformer timestamp? time-unparser)
    #(gen/return (t/now))))

(defn date?
  [x]
  (if (or (instance? org.joda.time.DateMidnight x)
          (time/midnight-timestamp? x))
    x
    (try
      (if (string? x)
        (date-parser x)
        :clojure.spec.alpha/invalid)
      (catch IllegalArgumentException e
        :clojure.spec.alpha/invalid))))

(def date
  (s/with-gen
    (s/conformer date? date-unparser)
    #(gen/return (t/today-at-midnight))))

(def uuid?
  (-regex? #"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))

(def uuid 
  (s/with-gen 
     (s/conformer uuid? identity)
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
                      :clojure.spec.alpha/invalid))
                  (catch Exception e
                    :clojure.spec.alpha/invalid))
    :else :clojure.spec.alpha/invalid))

(def ns-keyword
  (s/with-gen
    (s/conformer ns-keyword? identity)
    #(gen/such-that namespace (gen/keyword-ns))))

(defn -not-empty-string?
  [x]
  (and (string? x)
       (not-empty x)))

(def not-empty-string
  (s/with-gen 
    (s/conformer -not-empty-string? identity)
    #(gen/not-empty (s/gen string?))))
