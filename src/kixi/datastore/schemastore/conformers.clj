(ns kixi.datastore.schemastore.conformers
  (:require [clojure.core :exclude [integer?]]
            [clojure.spec :as s]))

(defn str-double->int
  "1. or 1.0...0 will convert to 1"
  [s]
  (some-> (re-find #"^([0-9]+)\.0*$" s)
          (last)
          (Integer/valueOf)))

(defn double->int
  "1.0 will convert to 1; anything else will be rejected"
  [d]
  (let [[integer decimal] (clojure.string/split (str d) #"\.")]
    (when (re-find #"^0+$" decimal)
      (int d))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Integer

(defn -integer?
  [x]
  (cond (clojure.core/integer? x) x
        (and (clojure.core/double? x)
             (double->int x))     (double->int x)
        (and (string? x)
             (str-double->int x)) (str-double->int x)
        (string? x) (try
                      (Integer/valueOf x)
                      (catch Exception e
                        :clojure.spec/invalid))
        :else :clojure.spec/invalid))

(def integer? (s/conformer -integer?))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Double

(defn -double?
  [x]
  (cond
    (clojure.core/double? x) x
    (clojure.core/integer? x) (double x)
    (string? x) (try
                  (Double/valueOf x)
                  (catch Exception e
                    :clojure.spec/invalid))
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
    (if (and (string? x) (re-find rs x)) x :clojure.spec/invalid)))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; String

(def string? (s/conformer (-regex? #".*")))
