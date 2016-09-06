(ns kixi.integration.local
  (:require [clojure.test :refer :all]
            [kixi.integration.upload-test]
            [kixi.repl :as repl]))

(defn cycle-system-fixture
  [all-tests]
  (repl/start)
  (all-tests)
  (repl/stop))

(use-fixtures :once cycle-system-fixture)

(deftest run-locally
  (is (successful? (run-tests 'kixi.integration.upload-test))))
