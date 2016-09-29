(ns kixi.integration.metadata-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]))

(use-fixtures :once cycle-system-fixture)

(deftest unknown-file-404
    (let [sr (get-metadata "foo")]
    (is (= 404
           (:status sr)))))

