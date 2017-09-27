(ns kixi.unit.transport-specs-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [kixi.datastore
             [schemastore :as ss]
             [transport-specs :as ts :refer [add-ns-to-keys]]]))

(stest/instrument `ts/filemetadata-transport->internal
                  `ts/schema-transport->internal)

(def sample-size (Integer/valueOf (str (env :generative-testing-size "100"))))

(defn check
  [sym]
  (-> sym
      (stest/check {:clojure.spec.test.alpha.check/opts {:num-tests sample-size}})
      first
      stest/abbrev-result
      :failure))

(deftest test-filemetadata-transport->internal
  (is (nil? (check `ts/filemetadata-transport->internal))))


(deftest add-schemastore-keywords-simple
  (let [r (add-ns-to-keys ::ss/_ {:foo 1 :bar 2 :baz 3})]
    (is (= 1 (::ss/foo r)))
    (is (= 2 (::ss/bar r)))
    (is (= 3 (::ss/baz r)))))

(deftest add-schemastore-keywords-nested
  (let [r (add-ns-to-keys ::ss/_ {:foo 1 :bar {:baz 2}})]
    (is (= 1 (::ss/foo r)))
    (is (= 2 (get-in r [::ss/bar ::ss/baz])))))

(deftest add-schemastore-keywords-nestedx2
  (let [r (add-ns-to-keys ::ss/_ {:foo 1 :bar {:baz {:quaz 2}}})]
    (is (= 1 (::ss/foo r)))
    (is (= 2 (get-in r [::ss/bar ::ss/baz ::ss/quaz])))))

(deftest add-schemastore-keywords-not-vectors
  (let [r (add-ns-to-keys ::ss/_ {:foo 1 :bar [:a 1 :b 2]})]
    (is (= 1 (::ss/foo r)))
    (is (= [:a 1 :b 2] (::ss/bar r)))))

(deftest add-schemastore-epic-test
  (let [r (add-ns-to-keys ::ss/_ {:name ::good-spec-c
                                      :type "list"
                                      :definition [:foo {:type "integer"}
                                                   :bar {:type "integer"}
                                                   :baz {:type "integer-range"
                                                         :min 3
                                                         :max 10}]})]
    (is (= "list" (::ss/type r)))
    (is (= :bar (get-in r [::ss/definition 2])))
    (is (= 10 (get-in r [::ss/definition 5 ::ss/max])))))
