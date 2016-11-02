(ns kixi.integration.metadata-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.web-server :as ws]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.datastore.schemastore :as ss]))

(alias 'ms 'kixi.datastore.metadatastore)

(def metadata-file-schema-id (atom nil))
(def metadata-file-schema {:name ::metadata-file-schema
                           :type "list"
                           :definition [:cola {:type "integer"}
                                        :colb {:type "integer"}]})

(def uid (uuid))

(defn setup-schema
  [all-tests]
  (let [r (post-spec-and-wait metadata-file-schema uid)]
    (if (= 202 (:status r))
      (reset! metadata-file-schema-id (extract-id r))
      (throw (Exception. (str "Couldn't post metadata-file-schema. Resp: " r)))))
  (all-tests))

(use-fixtures :once cycle-system-fixture setup-schema)

(deftest unknown-file-401
  (let [sr (get-metadata "foo" uid)]
    (is (= 401
           (:status sr)))))

(deftest small-file
  (let [pfr (post-file "./test-resources/metadata-one-valid.csv"
                       @metadata-file-schema-id
                       uid)]
    (when-created pfr
      (let [metadata-response (wait-for-metadata-key (extract-id pfr) ::ms/structural-validation uid)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id pfr)
                 ::ss/id @metadata-file-schema-id
                 ::ms/type "csv",
                 ::ms/name "foo",
                 ::ms/header true
                 ::ms/size-bytes 14,
                 ::ms/provenance {::ms/source "upload"
                                  ::ms/pieces-count 1}
                 ::ms/structural-validation {::ms/valid true}}}
         metadata-response)))))

(deftest small-file-invalid-schema
  (let [pfr (post-file "./test-resources/metadata-one-valid.csv"
                       "003ba24c-2830-4f28-b6af-905d6215ea1c"
                       uid) ;; schema doesn't exist
        ]
    (is-submap
     {:status 400
      :body {::ws/error "unknown-schema"}}
     pfr)))

(deftest small-file-invalid-data
  (let [pfr (post-file "./test-resources/metadata-one-invalid.csv"
                       @metadata-file-schema-id
                       uid)]
    (when-created pfr
      (let [metadata-response (wait-for-metadata-key (extract-id pfr) ::ms/structural-validation uid)]
        (is-submap
         {:status 200
          :body {::ms/id (extract-id pfr)
                 ::ss/id @metadata-file-schema-id
                 ::ms/type "csv",
                 ::ms/name "foo",
                 ::ms/size-bytes 14,
                 ::ms/provenance {::ms/source "upload"
                                  ::ms/pieces-count 1}
                 ::ms/structural-validation {::ms/valid false}}}
         metadata-response)))))
