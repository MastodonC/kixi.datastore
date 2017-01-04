(ns kixi.integration.metadata-test
  (:require [clojure.spec.test :refer [with-instrument-disabled]]
            [clojure.test :refer :all]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]]
            [kixi.integration.base :as base :refer :all]))

(alias 'ms 'kixi.datastore.metadatastore)

(defn metadata-file-schema
  [uid]
  {::ss/name ::metadata-file-schema
   ::ss/schema {::ss/type "list"
                ::ss/definition [:cola {::ss/type "integer"}
                                 :colb {::ss/type "integer"}]}
   ::ss/provenance {::ss/source "upload"
                    :kixi.user/id uid}
   ::ss/sharing {::ss/read [uid]
                 ::ss/use [uid]}})

(def get-schema-id (comp schema->schema-id metadata-file-schema))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest unknown-file-401
  (let [uid (uuid)
        sr (get-metadata uid "foo")]
    (unauthorised sr)))

(deftest small-file
  (let [uid (uuid)
        schema-id (get-schema-id uid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"
                            schema-id))]
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/structural-validation)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id metadata-response)
                 ::ms/schema {::ss/id schema-id
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
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
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
  (let [uid (uuid)
        schema-id (get-schema-id uid)
        metadata-response (send-file-and-metadata
                           (assoc (create-metadata
                                   uid
                                   "./test-resources/metadata-one-valid-no-header.csv"
                                   schema-id)
                                  ::ms/header false))]
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/structural-validation)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id metadata-response)
                 ::ms/schema {::ss/id schema-id
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
  (let [uid (uuid)]
    (is-file-metadata-rejected
     uid
     #(send-file-and-metadata-no-wait
       (create-metadata
        uid
        "./test-resources/metadata-one-valid.csv"
        "003ba24c-2830-4f28-b6af-905d6215ea1c")) ;; schema doesn't exist
     {:reason :schema-unknown})))

(comment "with-instument-disabled just doesn't seem to work here. Investigate!"
         (deftest small-file-invalid-metadata
           (with-instrument-disabled
             (let [uid (uuid)
                   resp (send-file-and-metadata
                         (dissoc
                          (create-metadata
                           uid
                           "./test-resources/metadata-one-valid.csv"
                           @metadata-file-schema-id)
                          ::ms/size-bytes))]
               (is-submap {:reason :metadata-invalid}
                          (:kixi.comms.event/payload resp))))))

(deftest small-file-invalid-data
  (let [uid (uuid)
        schema-id (get-schema-id uid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-invalid.csv"
                            schema-id))]
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/structural-validation)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id metadata-response)
                 ::ms/schema {:kixi.user/id uid
                              ::ss/id schema-id}
                 ::ms/type "stored"
                 ::ms/file-type "csv"
                 ::ms/name "./test-resources/metadata-one-invalid.csv"
                 ::ms/size-bytes 14,
                 ::ms/provenance {:kixi.user/id uid
                                  ::ms/source "upload"}
                 ::ms/structural-validation {::ms/valid false}}}
         metadata-response)))))
