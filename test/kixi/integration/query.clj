(ns kixi.integration.query
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

(def uid (uuid))

;(def post-file (partial base/post-file uid))
;(def wait-for-metadata-key (partial base/wait-for-metadata-key uid))

(def metadata-file-schema-id (atom nil))
(def metadata-file-schema {::ss/name ::metadata-file-schema
                           ::ss/schema {::ss/type "list"
                                        ::ss/definition [:cola {::ss/type "integer"}
                                                         :colb {::ss/type "integer"}]}
                           ::ss/sharing {::ss/read [uid]
                                         ::ss/use [uid]}})

(use-fixtures :once
  cycle-system-fixture
  extract-comms
  (setup-schema uid metadata-file-schema metadata-file-schema-id))

(deftest novel-user-finds-nothing
  (is-submap {:items []
              :paging {:total 0 
                       :count 0
                       :index 0}}
             (:body
              (search-metadata (uuid) [::ms/file-read]))))

(deftest unknown-activity-errors-nicely)

(deftest only-one-group-of-many-matches)

(deftest novel-user-finds-nothing-when-there-is-something
    (let [metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"
                            @metadata-file-schema-id))]
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/structural-validation)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id metadata-response)
                 ::ms/schema {::ss/id @metadata-file-schema-id
                              :kixi.user/id uid}
                 ::ms/type "stored"
                 ::ms/file-type "csv"
                 ::ms/name "./test-resources/metadata-one-valid.csv"
                 ::ms/header true
                 ::ms/size-bytes 14
                 ::ms/provenance {:kixi.user/id uid
                                  ::ms/source "upload"}
                 ::ms/structural-validation {::ms/valid true}}}
         metadata-response)))))

(deftest meta-read-user-reads-without-sending-activities
  
  (is (= (search (uuid) [])
         {:count 0})))

(deftest paging)
