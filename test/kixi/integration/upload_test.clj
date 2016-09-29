(ns kixi.integration.upload-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all])
  (:import [java.io 
            File
            FileNotFoundException]))

(use-fixtures :once cycle-system-fixture)

(deftest round-trip-files
  (let [r (post-file "./test-resources/10B-file.txt")]
    (is (= 201
           (:status r))
        (parse-json (:body r)))    
    (is (dload-file (get-in r [:headers "Location"]))))
  (let [r (post-file "./test-resources/10MB-file.txt")]
    (is (= 201
           (:status r))
        (parse-json (:body r)))
    (is (files-match?
         "./test-resources/10MB-file.txt"
         (dload-file (get-in r [:headers "Location"])))))
  (let [r (post-file "./test-resources/300MB-file.txt")]
    (is (= 201
           (:status r))
        (parse-json (:body r)))
    (is (files-match? 
         "./test-resources/300MB-file.txt"
         (dload-file (get-in r [:headers "Location"]))))))
