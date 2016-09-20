(ns kixi.integration.base
  (:require [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [kixi.repl :as repl]))


(defn cycle-system-fixture
  [all-tests]
  (repl/start)
  (all-tests)
  (repl/stop))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defmacro deftest-broken
  [name & everything]
  `(clojure.test/deftest ~(vary-meta name assoc :integration true) ~@everything))

(defn service-url
  []
  (or (System/getenv "SERVICE_URL") "localhost:8080"))
