(ns kixi.integration.metadata-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :as base :refer :all :exclude [post-file]]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.web-server :as ws]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.datastore.schemastore :as ss]))

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

(defn setup-schema
  [all-tests]
  (let [r (post-spec uid metadata-file-schema)]
    (if (= 202 (:status r))
      (reset! metadata-file-schema-id (extract-id-location r))
      (throw (Exception. (str "Couldn't post metadata-file-schema. Resp: " r)))))
  (all-tests))

(use-fixtures :once cycle-system-fixture setup-schema extract-comms)

(deftest unknown-file-401
  (let [sr (get-metadata uid "foo")]
    (unauthorised sr)))

(deftest small-file
  (let [metadata-response (deliver-file-and-metadata
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
  (let [metadata-response (deliver-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/id)]
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
         metadata-response)
        (is (nil? (::ms/schema metadata-response)))
        (is (nil? (::ms/structural-validation metadata-response)))))))

(deftest small-file-no-header
  (let [metadata-response (deliver-file-and-metadata
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
  (let [metadata-response (deliver-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"
                            "003ba24c-2830-4f28-b6af-905d6215ea1c"))]  ;; schema doesn't exist
    (when-success metadata-response
      (let [metadata-response (wait-for-metadata-key uid (extract-id metadata-response) ::ms/structural-validation)]
        (is-submap
         {:status 400
          :body {::ws/error "unknown-schema"}}
         metadata-response)))))

(comment

  (deftest small-file-invalid-schema
    (let [pfr (post-file "./test-resources/metadata-one-valid.csv"
                         "003ba24c-2830-4f28-b6af-905d6215ea1c") ;; schema doesn't exist
          ]
      (is-submap
       {:status 400
        :body {::ws/error "unknown-schema"}}
       pfr)))

  (deftest small-file-invalid-data
    (let [pfr (post-file "./test-resources/metadata-one-invalid.csv"
                         @metadata-file-schema-id)]
      (when-created pfr
        (let [metadata-response (wait-for-metadata-key (extract-id pfr) ::ms/structural-validation)]
          (is-submap
           {:status 200
            :body {::ms/id (extract-id pfr)
                   ::ms/schema {:kixi.user/id uid
                                ::ss/id @metadata-file-schema-id}
                   ::ms/type "csv",
                   ::ms/name "./test-resources/metadata-one-invalid.csv"
                   ::ms/size-bytes 14,
                   ::ms/provenance {:kixi.user/id uid
                                    ::ms/source "upload"
                                    ::ms/pieces-count 1}
                   ::ms/structural-validation {::ms/valid false}}}
           metadata-response)))))
  )
