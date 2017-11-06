(ns kixi.unit.filestore.command-handler
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [kixi.datastore.filestore.command-handler :refer :all]))

(deftest calc-chunk-ranges-test
  (testing "single result"
    (let [ranges (calc-chunk-ranges 10 9)]
      (is (= [{:start-byte 0  :length-bytes 9}] ranges))))
  (testing "small range"
    (let [ranges (calc-chunk-ranges 10 34)]
      (is (= [{:start-byte 0  :length-bytes 10}
              {:start-byte 10 :length-bytes 10}
              {:start-byte 20 :length-bytes 10}
              {:start-byte 30 :length-bytes 4}] ranges))))
  (testing "bigger range"
    (let [ranges (calc-chunk-ranges 100 648)]
      (is (= [{:start-byte 0  :length-bytes 100}
              {:start-byte 100 :length-bytes 100}
              {:start-byte 200 :length-bytes 100}
              {:start-byte 300 :length-bytes 100}
              {:start-byte 400 :length-bytes 100}
              {:start-byte 500 :length-bytes 100}
              {:start-byte 600 :length-bytes 48}] ranges)))))

(deftest add-ns-test
  (let [x (add-ns :foo {:a 1 :b 2 :c {:x 3}})]
    (is (= {:foo/a 1 :foo/b 2 :foo/c {:x 3}} x))))
