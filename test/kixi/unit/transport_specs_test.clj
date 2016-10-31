(ns kixi.unit.transport-specs-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.spec :as s]
            [clojure.spec.test :as stest]
            [clojure.spec.gen :as gen]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.transport-specs :as ts]))

(stest/instrument `ts/filemetadata-transport->internal)

(def sample-size (Integer/valueOf (str (env :generative-testing-size "1000"))))

(deftest test-filemetadata-transport->internal
  (is (nil? (:failure
             (stest/abbrev-result (first (stest/check `ts/filemetadata-transport->internal {:num-tests sample-size})))))))
