(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clj-http.client :as client]
            [kixi.integration.base :refer [service-url cycle-system-fixture uuid
                                           post-spec get-spec extract-spec]]))

(def small-segmentable-file-schema `(clojure.spec/cat :cola 'int?
                                                      :colb 'int?
                                                      :colc 'int?))

(comment "Need to try the above spec with sub spec")
(comment "Need schema failure tests")

(use-fixtures :once cycle-system-fixture)

(deftest unknown-spec-404
  (let [r-g (get-spec :foo)]
    (is (= 404
           (:status r-g)))))

(deftest round-trip-predicate-only-spec
  (let [r-p (post-spec :integer 'integer?)
        r-g (get-spec :integer)]
    (is (= 200
           (:status r-p)))
    (is (= 200
           (:status r-g)))
    (is (= 'integer?
           (extract-spec r-g)))))

