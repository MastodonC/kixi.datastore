(ns kixi.unit.metrics-tests
  (:require [clojure.test :refer :all]
            [kixi.datastore.metrics :refer :all]))

(deftest ensure-uri-names-are-clean
  (let [uri "file/08eb375b-bf92-40cc-bced-e7bf74a7ceba/meta"
        method "GET"
        status "200"]
    (is (= (uri-method-status->metric-name
            uri
            method
            status)
           ["resources" "file.GUID.meta" "GET.200"]))))

