(ns kixi.integration.local
  (:require [clojure.test :refer :all]
            [kixi.integration.upload-test]
            [kixi.repl :as repl]))


#_(deftest run-locally
  (is (successful? (run-tests 'kixi.integration.upload-test))))
