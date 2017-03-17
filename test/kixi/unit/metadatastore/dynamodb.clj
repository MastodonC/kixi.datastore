(ns kixi.unit.metadatastore.dynamodb
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.data :as data]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore.dynamodb :refer :all]))

(defn slow-keep-if-count
  "Expensive implementation, should take advantage of ordering"  
  [target-cnt ids]
  (->> ids
       (group-by identity)
       vals
       (filter #(>= (count %) target-cnt))
       (mapcat (fn [wants]
                 (if (>= target-cnt
                        (count wants))
                   (repeat (quot target-cnt
                                 (count wants))
                           (first wants))
                   wants)))
       seq))

(deftest enforce-and-sematics-works
  (doseq [cnt [1 2]]
    (doseq [subject [[1 2 3] [1 2 2 3]]]
      (is (= (slow-keep-if-count cnt subject)
             (keep-if-at-least cnt subject))
          (str "With " cnt subject)))))

(deftest knit-ordered-data-works
  (let [one [1 2 4 5]
        two [1 3 4]]
    (is (= [1 1 2 3 4 4 5]
           (knit-ordered-data (fn [a b] 
                                (cond
                                  (nil? b) a
                                  (nil? a) b 
                                  :else (< a b))) [one two])))))
