(ns kixi.integration.kaylee-test
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [kaylee :as kaylee]
             [metadatastore :as ms]]
            [kixi.integration.base :as base :refer :all]))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest kaylee-get-metadata
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])]
        (is (= (kaylee/get-metadata meta-id)
               (:body metadata-response)))))))

(deftest kaylee-sharing-update
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (kaylee/send-sharing-update uid meta-id ::ms/sharing-conj ::ms/meta-read new-group)]
        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                          (= #{uid new-group}
                             (set (get-in metadata [:body ::ms/sharing ::ms/meta-read])))))
        (let [updated-metadata (get-metadata uid meta-id)]
          (is (= #{uid new-group}
                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read])))))))))

(deftest kaylee-remove-all-sharing
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            _ (kaylee/remove-all-sharing uid meta-id)]
        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                          (= #{}
                             (set (get-in metadata [:body ::ms/sharing ::ms/meta-read])))))
        (let [updated-metadata (get-metadata uid meta-id)]
          (is (= #{}
                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read]))))
          (is (= #{}
                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-update]))))
          (is (= #{}
                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/file-read])))))))))
