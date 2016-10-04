(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clojure.spec :as s]
            [clj-http.client :as client]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.integration.base :refer [service-url cycle-system-fixture uuid
                                           post-spec get-spec extract-spec]]))

(use-fixtures :once cycle-system-fixture)

(deftest unknown-spec-404
  (let [r-g (get-spec :foo)]
    (is (= 404
           (:status r-g)))))

(deftest repost-spec-400
  (let [r-g1 (post-spec :post-me-123 'conformers/integer?)
        r-g2 (post-spec :post-me-123 'conformers/integer?)]
    (is (= 200
           (:status r-g1)))
    (is (= 400
           (:status r-g2)))))

(deftest good-spec-200
  (let [r (post-spec ::good-spec `(clojure.spec/cat :foo conformers/integer?))]
    (is (= 200 (:status r))  "Good spec")))

(deftest bad-spec-400
  (is (= 404 (:status (post-spec :bad-spec  `(clojure.spec/cat :foo conformers/integer?))))   "Unnamespaced name")
  (is (= 400 (:status (post-spec ::bad-spec `(clojure.spec/cat :foo clojure.core/integer?)))) "Illegal namespace symbol")
  (is (= 400 (:status (post-spec ::bad-spec `(launch-missiles!))))                            "Illegal function call")
  (is (= 400 (:status (post-spec ::bad-spec `(clojure.spex/cat :foo conformers/integer?))))   "Mistyped namespace"))


(deftest round-trip-predicate-only-spec
  (let [r-p (post-spec :integer 'integer?)
        r-g (get-spec :integer)]
    (is (= 200
           (:status r-p)))
    (is (= 200
           (:status r-g)))
    (is (= 'integer?
           (extract-spec r-g)))))

(defn resolve-spec
  [spec-sym get-spec-fn]
  (let [[initial & forms] spec-sym]
    (when (= initial 'clojure.spec/cat)
      (doseq [f (->> forms
                     (partition 2)
                     (map second))]
        (let [inner-spec (get-spec-fn f)]
          (s/def-impl f inner-spec (eval inner-spec)))))
    (s/spec (eval spec-sym))))

(deftest round-trip-composite-spec
  (let [x-name      :kixi.datastore.spec/wrap-integer
        y-name      :kixi.datastore.spec/wrap-wrap-integer
        x-spec      (post-spec x-name `conformers/integer?)
        y-spec      (post-spec y-name `(clojure.spec/cat :foo ~x-name))
        y-r         (get-spec y-name)
        extracted-y (extract-spec y-r)
        y-spec      (resolve-spec extracted-y (comp extract-spec get-spec))]
    (is (= 200
           (:status y-r)))
    (is (= `(clojure.spec/cat :foo ~x-name)
           extracted-y))
    (is (s/valid? y-spec [123]))
    (is (s/valid? y-spec ["123"]))
    (is (not (s/valid? y-spec ["x"])))))
