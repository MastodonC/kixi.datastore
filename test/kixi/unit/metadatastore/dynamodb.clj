(ns kixi.unit.metadatastore.dynamodb
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.data :as data]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore.dynamodb :refer :all]))

(defn slow-keep-if-count
  "Expensive implementation, should take advantage of ordering."  
  [target-cnt ids]
  (->> ids
       (group-by identity)
       vals
       (filter #(>= (count %) target-cnt))
       (map first)
       (sort >)
       seq))

(deftest enforce-and-sematics-works
  (checking ""
            1000
            [cnt (gen/choose 1 10)
             subject (gen/vector (gen/choose 1 100) 0 50)]
            (let [ss (sort > subject)]
              (is (= (slow-keep-if-count cnt ss)
                     (keep-if-at-least cnt ss))
                  (str "With " cnt ss)))))

(deftest knit-ordered-data-works
  (let [one [1 2 4 5]
        two [1 3 4]]
    (is (= [1 1 2 3 4 4 5]
           (knit-ordered-data (fn [a b] 
                                (cond
                                  (nil? b) a
                                  (nil? a) b 
                                  :else (< a b))) [one two])))))
