(ns kixi.integration.upload-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [filestore :as fs]
             [metadatastore :as ms]]
            [kixi.integration.base :refer :all]))

(use-fixtures :once cycle-system-fixture extract-comms)

(deftest create-link-test
  (let [uid (uuid)
        link (get-upload-link-event uid)]
    (is link)
    (when-let [r link]
      (is (get-in r [:kixi.comms.event/payload :kixi.datastore.filestore/upload-link]))
      (is (get-in r [:kixi.comms.event/payload :kixi.datastore.filestore/id])))))

(deftest round-trip-small-file
  (let [uid (uuid)
        md-resp (send-file-and-metadata
                 (create-metadata uid
                                  "./test-resources/metadata-one-valid.csv"))]
    (when-success md-resp
      (let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-one-valid.csv"
             (dload-file uid locat)))))))

(deftest round-trip-12M-file
  (let [uid (uuid)
        md-resp (send-file-and-metadata
                 (create-metadata uid
                                  "./test-resources/metadata-12MB-valid.csv"))]
    (when-success md-resp
      (let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-12MB-valid.csv"
             (dload-file uid locat)))))))

(deftest round-trip-344M-file
  (let [uid (uuid)
        md-resp (send-file-and-metadata
                 (create-metadata uid
                                  "./test-resources/metadata-344MB-valid.csv"))]
    (when-success md-resp
      (let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-344MB-valid.csv"
             (dload-file uid locat)))))))

(deftest multi-part-round-trip-12MB-file
  (let [uid (uuid)
        md-resp (send-multi-part-file-and-metadata
                 (create-metadata uid
                                  "./test-resources/metadata-12MB-valid.csv"))]
    (when-success md-resp
      (let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-12MB-valid.csv"
             (dload-file uid locat)))))))

(deftest rejected-when-file-not-uploaded
  (let [uid (uuid)]
    (is-file-metadata-rejected
     uid
     #(send-metadata-cmd uid
                         (assoc (create-metadata uid
                                                 "./rejected-when-file-not-uploaded.non-file")
                                ::ms/id (uuid)))
     {:reason :file-not-exist})))

(deftest rejected-when-size-incorrect
  (let [uid (uuid)]
    (is-file-metadata-rejected
     uid
     #(send-file-and-metadata-no-wait
       (assoc (create-metadata uid
                               "./test-resources/metadata-one-valid.csv")
              ::ms/size-bytes 1))
     {:reason :file-size-incorrect
      :actual 14
      :expected 1})))
