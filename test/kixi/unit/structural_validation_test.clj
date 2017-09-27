(ns kixi.unit.structural-validation-test
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [kixi.datastore.structural-validation :refer :all]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.metadatastore :as ms]))

(deftest csv-schema-test-small-metadata
  (is (= {::ms/valid true}
         (csv-schema-test (s/cat :cola sc/integer?
                                 :colb sc/integer?)
                          "./test-resources/metadata-one-valid.csv"
                          true))))

(deftest csv-schema-test-small-metadata-no-header
  (is (= {::ms/valid true}
         (csv-schema-test (s/cat :cola sc/integer?
                                 :colb sc/integer?)
                          "./test-resources/metadata-one-valid-no-header.csv"
                          false))))

#_(deftest csv-schema-test-large
  (is (= {::ms/valid true}
         (csv-schema-test (s/tuple sc/integer? sc/integer?)
                          "./test-resources/metadata-344MB-valid.csv"))))

#_(deftest csv-schema-test-large
  (is (= {::ms/valid true}
         (csv-schema-test (s/cat :cola sc/integer?
                                 :colb sc/integer?)
                          "./test-resources/metadata-344MB-valid.csv"))))

#_(deftest csv-schema-fail-test-large
  (is (= {::ms/valid true}
         (csv-schema-test (s/cat :cola sc/integer?
                                 :colb sc/integer?)
                          "./test-resources/metadata-344MB-invalid.csv"))))
