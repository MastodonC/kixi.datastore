(ns kixi.integration.upload-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all])
  (:import [java.io 
            File
            FileNotFoundException]))

(use-fixtures :once cycle-system-fixture)

(deftest round-trip-files
  (let [r (post-file "./test-resources/10B-file.txt")     
        f (dload-file (get-in r [:headers "Location"]))]
    (is (= 201
           (:status r)))
    (is (files-match?  "./test-resources/10B-file.txt"
                       f)))
  (let [r (post-file "./test-resources/10MB-file.txt")
        f (dload-file (get-in r [:headers "Location"]))]
    (is (= 201
           (:status r)))
    (is (files-match?  "./test-resources/10MB-file.txt"
                       f)))
  (let [r (post-file "./test-resources/300MB-file.txt")
        ^File f (dload-file (get-in r [:headers "Location"]))]
    (is (= 201
           (:status r)))
    (is (files-match?  "./test-resources/300MB-file.txt"
                       f))
    (.delete f)))
