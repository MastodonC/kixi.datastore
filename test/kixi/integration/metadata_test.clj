(ns kixi.integration.metadata-test
  (:require [clojure.test :refer :all]
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
(def metadata-file-schema {:name ::metadata-file-schema
                           :schema {:type "list"
                                    :definition [:cola {:type "integer"}
                                                 :colb {:type "integer"}]}
                           :sharing {:read [uid]
                                     :use [uid]}})

(use-fixtures :once
  cycle-system-fixture
  extract-comms
  (setup-schema uid metadata-file-schema metadata-file-schema-id))

(deftest unknown-file-401
  (let [sr (get-metadata uid "foo")]
    (unauthorised sr)))

(deftest small-file
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

(deftest small-file-no-schema
  (let [metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (is-submap
       {:status 200
        :body {::ms/id (extract-id metadata-response)
               ::ms/type "stored"
               ::ms/file-type "csv"
               ::ms/name "./test-resources/metadata-one-valid.csv"
               ::ms/header true
               ::ms/size-bytes 14
               ::ms/provenance {:kixi.user/id uid
                                ::ms/source "upload"}}}
       metadata-response))))

(deftest small-file-no-header
  (let [metadata-response (send-file-and-metadata
                           (assoc (create-metadata
                                   uid
                                   "./test-resources/metadata-one-valid-no-header.csv"
                                   @metadata-file-schema-id)
                                  ::ms/header false))]
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/structural-validation)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id metadata-response)
                 ::ms/schema {::ss/id @metadata-file-schema-id
                              :kixi.user/id uid}
                 ::ms/type "stored"
                 ::ms/file-type "csv"
                 ::ms/name "./test-resources/metadata-one-valid-no-header.csv"
                 ::ms/header false
                 ::ms/size-bytes 4
                 ::ms/provenance {:kixi.user/id uid
                                  ::ms/source "upload"}
                 ::ms/structural-validation {::ms/valid true}}}
         metadata-response)))))

(deftest small-file-invalid-schema
  (is-file-metadata-rejected 
   #(send-file-and-metadata-no-wait
     (create-metadata
      uid
      "./test-resources/metadata-one-valid.csv"
      "003ba24c-2830-4f28-b6af-905d6215ea1c")) ;; schema doesn't exist
   {:reason :schema-unknown}))

(deftest small-file-invalid-data
  (let [metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-invalid.csv"
                            @metadata-file-schema-id))]
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/structural-validation)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id metadata-response)
                 ::ms/schema {:kixi.user/id uid
                              ::ss/id @metadata-file-schema-id}
                 ::ms/type "stored"
                 ::ms/file-type "csv"
                 ::ms/name "./test-resources/metadata-one-invalid.csv"
                 ::ms/size-bytes 14,
                 ::ms/provenance {:kixi.user/id uid
                                  ::ms/source "upload"}
                 ::ms/structural-validation {::ms/valid false}}}
         metadata-response)))))

