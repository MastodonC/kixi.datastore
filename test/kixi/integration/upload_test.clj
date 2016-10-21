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

(def irrelevant-schema-id (atom nil))
(def irrelevant-schema {:name ::irrelevant-schema
                        :type "list"
                        :definition [:cola {:type "integer"}]})


(defn setup-schema
  [all-tests]
  (let [r (post-spec irrelevant-schema)]
    (if (= 202 (:status r))
      (reset! irrelevant-schema-id (extract-id r))
      (throw (Exception. "Couldn't post irrelevant-schema")))
    (wait-for-url (get-in r [:headers "Location"])))
  (all-tests))

(use-fixtures :once cycle-system-fixture setup-schema)

(deftest round-trip-files
  (let [r (post-file "./test-resources/10B-file.txt"
                     @irrelevant-schema-id)]
    (is (= 201
           (:status r))
        (str "Reason: " (parse-json (:body r))))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/10B-file.txt"
           (dload-file locat)))))
  (let [r (post-file "./test-resources/10MB-file.txt"
                     @irrelevant-schema-id)]
    (is (= 201
           (:status r))
        (str "Reason: " (parse-json (:body r))))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/10MB-file.txt"
           (dload-file locat)))))
  (let [r (post-file "./test-resources/300MB-file.txt"
                     @irrelevant-schema-id)]
    (is (= 201
           (:status r))
        (str "Reason: " (parse-json (:body r))))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/300MB-file.txt"
           (dload-file locat))))))
