(ns kixi.integration.query-test
  (:require [clojure.test :refer :all]
            [clojure.spec.test :refer [with-instrument-disabled]]
            [clj-http.client :as client]
            [kixi.datastore
             [metadata-creator :as mdc]
             [metadatastore :as ms]
             [schemastore :as ss]
             [web-server :as ws]]
            [kixi.integration.base :as base :refer :all]))


(alias 'ms 'kixi.datastore.metadatastore)

(use-fixtures :once
  cycle-system-fixture
  extract-comms)


(deftest novel-user-finds-nothing
  (is-submap {:items []
              :paging {:total 0 
                       :count 0
                       :index 0}}
             (:body
              (search-metadata (uuid) [::ms/file-read]))))

(deftest unknown-activity-errors-nicely
  (is-submap {:status 400
              :body {:kixi.datastore.web-server/error "query-invalid"}}
             (search-metadata (uuid) [::ms/Xfile-readX])))

(deftest meta-read-activity-added-to-queries-by-default
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (is-submap {:status 200
                  :body {:items [{::ms/type "stored"
                                  ::ms/sharing {::ms/meta-read [uid]}}]}}
                 (search-metadata uid [])))))

(deftest multiple-groups-can-have-read
  (let [uid (uuid)
        group1 (uuid)
        metadata-response (send-file-and-metadata
                           (update-in (create-metadata
                                       uid
                                       "./test-resources/metadata-one-valid.csv")
                                      [::ms/sharing ::ms/meta-read]
                                      conj group1))]
    (when-success metadata-response
      (is-submap {:status 200
                  :body {:items [{::ms/type "stored"
                                  ::ms/sharing {::ms/meta-read [uid group1]}}]}}
                 (search-metadata uid []))
      (is-submap {:status 200
                  :body {:items [{::ms/type "stored"
                                  ::ms/sharing {::ms/meta-read [uid group1]}}]}}
                 (search-metadata group1 [])))))

(deftest novel-user-finds-nothing-when-there-is-something
  (let [uid (uuid)
        novel-group (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (is-submap {:status 200
                  :body {:items []}}
                 (search-metadata novel-group [])))))

(deftest meta-read-explicitly-set
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (is-submap {:status 200
                  :body {:items [{::ms/type "stored"
                                  ::ms/sharing {::ms/meta-read [uid]}}]}}
                 (search-metadata uid [::ms/meta-read])))))

(deftest user-with-multiple-groups
  (let [uid (uuid)
        extra-group (uuid)
        metadata-response (send-file-and-metadata
                           uid
                           [uid extra-group]
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (is-submap {:status 200
                  :body {:items [{::ms/type "stored"
                                  ::ms/sharing {::ms/meta-read [uid]}}]}}
                 (search-metadata uid [::ms/meta-read])))))

(deftest search-uses-and-for-multiple-activities
  (let [uid (uuid)
        only-read-group (uuid)
        metadata-response (send-file-and-metadata
                           (update-in
                            (create-metadata
                             uid
                             "./test-resources/metadata-one-valid.csv")
                            [::ms/sharing ::ms/meta-read]
                            conj only-read-group))]
    (when-success metadata-response
      (is-submap {:status 200
                  :body {:items [{::ms/type "stored"
                                  ::ms/sharing {::ms/meta-read [uid only-read-group]
                                                ::ms/file-read [uid]}}]}}
                 (search-metadata uid [::ms/meta-read ::ms/file-read])
                 "multiple activities in query should be supported")
      (is-submap {:status 200
                  :body {:items [{::ms/type "stored"
                                  ::ms/sharing {::ms/meta-read [uid only-read-group]
                                                ::ms/file-read [uid]}}]}}
                 (search-metadata only-read-group [::ms/meta-read]))
      (is-submap {:status 200
                  :body {:items []}}
                 (search-metadata only-read-group [::ms/meta-read ::ms/file-read])
                 "activities should be AND'd together"))))

(deftest paging)
