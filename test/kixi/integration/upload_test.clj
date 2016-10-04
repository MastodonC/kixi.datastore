(ns kixi.integration.upload-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]
            [kixi.datastore.schemastore.conformers :as conformers])
  (:import [java.io
            File
            FileNotFoundException]))

(def irrelevant-schema `(clojure.spec/cat :cola conformers/integer?))

(defn setup-schema
  [all-tests]
  (post-spec ::irrelevant-schema irrelevant-schema)
  (all-tests))

(use-fixtures :once cycle-system-fixture setup-schema)

(deftest round-trip-files
  (let [r (post-file "./test-resources/10B-file.txt"
                     ::irrelevant-schema)]
    (is (= 201
           (:status r))
        (parse-json (:body r)))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/10B-file.txt"
           (dload-file locat)))))
  (let [r (post-file "./test-resources/10MB-file.txt"
                     ::irrelevant-schema)]
    (is (= 201
           (:status r))
        (parse-json (:body r)))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/10MB-file.txt"
           (dload-file locat)))))
  (let [r (post-file "./test-resources/300MB-file.txt"
                     ::irrelevant-schema)]
    (is (= 201
           (:status r))
        (parse-json (:body r)))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/300MB-file.txt"
           (dload-file locat))))))
