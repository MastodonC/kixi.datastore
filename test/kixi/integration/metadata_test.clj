(ns kixi.integration.metadata-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.datastore.schemastore :as ss]))

(alias 'ms 'kixi.datastore.metadatastore)

(def metadata-file-schema `(clojure.spec/cat :cola conformers/integer?
                                             :colb conformers/integer?))
(defn setup-schema
  [all-tests]
  (post-spec ::metadata-file-schema metadata-file-schema)
  (all-tests))

(use-fixtures :once cycle-system-fixture setup-schema)

(deftest unknown-file-404
    (let [sr (get-metadata "foo")]
    (is (= 404
           (:status sr)))))

(deftest small-file
  (let [pfr (post-file "./test-resources/metadata-test-file.csv"
                       ::metadata-file-schema)
        metadata-response (wait-for-metadata-key (extract-id pfr) :structural-validation)]
    (is-submap
     {:status 201}
     pfr)
    (is-submap
     {:status 200
      :body {::ms/id (extract-id pfr)
             ::ss/name (nskw->str ::metadata-file-schema)
             ::ms/type "csv",
             ::ms/name "foo",
             ::ms/size-bytes 13,
             ::ms/provenance {::ms/source "upload"
                              ::ms/pieces-count nil}
             :structural-validation {:valid true}}}
     metadata-response)))

(deftest small-file-invalid-schema
  (let [pfr (post-file "./test-resources/metadata-test-file.csv"
                       ::this-schema-doesnt-exist)
        metadata-response (wait-for-metadata-key (extract-id pfr) :structural-validation)]
    (is-submap
     {:status 400
      :body {:error "unknown-schema"}}
     pfr)))

(deftest small-file-invalid-data
  (let [pfr (post-file "./test-resources/metadata-test-file-invalid.csv"
                       ::metadata-file-schema)
        metadata-response (wait-for-metadata-key (extract-id pfr) :structural-validation)]
    (is-submap
     {:status 201}
     pfr)
    (is-submap
     {:status 200
      :body {::ms/id (extract-id pfr)
             ::ss/name (nskw->str ::metadata-file-schema)
             ::ms/type "csv",
             ::ms/name "foo",
             ::ms/size-bytes 14,
             ::ms/provenance {::ms/source "upload"
                              ::ms/pieces-count nil}
             :structural-validation {:valid false}}}
     metadata-response)))
