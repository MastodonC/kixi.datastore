(ns kixi.datastore.schemastore.conformers
  (:require [clojure.core :exclude [integer?]]
            [clojure.spec :as s]))

(defn -integer?
  [x]
  (if (clojure.core/integer? x)
    x
    (if (string? x)
      (try
        (Integer/valueOf x)
        (catch Exception e
          :clojure.spec/invalid))
      :clojure.spec/invalid)))

(def integer? (s/conformer -integer?))
