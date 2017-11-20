(ns kixi.unit.filestore.command-handler
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :as stest]
            [clojure.test.check :as tc]
            [kixi.datastore.filestore.command-handler :refer :all]))

(def sample-size 100)

(defn check
  [sym]
  (-> sym
      (stest/check {:clojure.spec.test.alpha.check/opts {:num-tests sample-size}})
      first
      stest/abbrev-result
      :failure))

(deftest check-calc-chunk-ranges
  (is (nil?
       (check `calc-chunk-ranges))))

(deftest add-ns-test
  (let [x (add-ns :foo {:a 1 :b 2 :c {:x 3}})]
    (is (= {:foo/a 1 :foo/b 2 :foo/c {:x 3}} x))))
