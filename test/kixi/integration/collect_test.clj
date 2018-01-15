(ns kixi.integration.collect-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [kixi.integration.base :as base :refer :all]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'ms 'kixi.datastore.metadatastore)

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(defn random-uuid-set
  ([]
   (random-uuid-set 10))
  ([n]
   (set (repeatedly (inc (rand-int n)) uuid))))

(deftest happy-collect-request
  (let [uid (uuid)
        dr (empty-datapack uid)
        message "foobar"
        groups (random-uuid-set)]
    (when-success dr
      (send-collection-request-cmd uid message groups (get-in dr [:body ::ms/id])))))
