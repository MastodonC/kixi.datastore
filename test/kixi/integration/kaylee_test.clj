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

(deftest kaylee-get-metadata-by-group
  (let [uid (uuid)
        total-files 15
        metadata (create-metadata uid "./test-resources/metadata-one-valid.csv")
        _ (->> (range total-files)
               (map (fn [_] 
                      (send-file-and-metadata-no-wait
                       metadata)))
               doall
               (map ::ms/id)
               (map #(wait-for-metadata-key uid % ::ms/id))
               doall)]
    (wait-for-pred
     #(= total-files
         (get-in (search-metadata uid [::ms/meta-read])
                 [:body :paging :total])))

    (is-submap {:paging {:total total-files
                         :count total-files}}
               (kaylee/get-metadata-by-group [uid] 0 50))
    (is-submap {:paging {:total total-files
                         :count 5}}
               (kaylee/get-metadata-by-group [uid] 0 5))))
