(ns kixi.unit.transport-specs-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.spec :as s]
            [clojure.spec.test :as stest]
            [clojure.spec.gen :as gen]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.transport-specs :as ts]
            [kixi.datastore.schemastore :as ss]))

(stest/instrument `ts/filemetadata-transport->internal
                  `ts/schema-transport->internal)

(def sample-size (Integer/valueOf (str (env :generative-testing-size "100"))))

(defn check
  [sym]
  (-> sym
      (stest/check {:clojure.spec.test.check/opts {:num-tests sample-size}})
      first
      stest/abbrev-result
      :failure))

(deftest test-filemetadata-transport->internal
  (is (nil? (check `ts/filemetadata-transport->internal))))


(deftest test-schema-transport->internal
  (is (nil? (check `ts/schema-transport->internal))))
