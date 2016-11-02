(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clj-http.client :as client]
            [kixi.datastore.web-server :refer [add-ns-to-keys]]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.integration.base :refer [service-url cycle-system-fixture uuid
                                           post-spec get-spec extract-schema parse-json
                                           wait-for-url is-submap extract-id
                                           get-spec-direct]]))

(def uuid-regex
  #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

(def uid (uuid))

(def location-regex
  (re-pattern (str #"schema\/" uuid-regex)))

(use-fixtures :once cycle-system-fixture)

(deftest good-round-trip
  (let [schema {:name ::new-good-spec-a
                :type "integer-range"
                :min 3
                :max 10}
        r1 (post-spec schema)]
    (is-submap {:status 202} r1)
    (if (= 202 (:status r1))
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)
            r2       (get-spec id uid)]
        (is (re-find location-regex location))
        (is (re-find uuid-regex id))
        (is-submap {:status 200} r2)
        (is-submap (add-ns-to-keys ::ss/_ (dissoc schema :name))
                   (::ss/schema (extract-schema r2)))))))

(deftest unknown-spec-404
  (let [r-g (get-spec-direct "c0bbb46f-9a31-47c2-b30c-62eba45470d4")]
    (is (= 404
           (:status r-g)))))

(comment "This next test is likely to be flaky as it depends on how quickly the events are consumed. Need an idempotent schema create...")
(deftest repost-spec-get-same-result
  (let [schema {:name ::reposted-a
                :type "integer"}
        r-g1 (post-spec schema)
        _ (wait-for-url (get-in r-g1 [:headers "Location"]) uid)
        r-g2 (post-spec schema)]
    (is (= 202
           (:status r-g1)))
    (is (= 202
           (:status r-g2)))
    (is (= (get-in r-g1 [:headers "Location"])
           (get-in r-g2 [:headers "Location"])))))

(deftest good-spec-202
  (is (= 202 (:status (post-spec {:name ::good-spec-a
                                  :type "integer-range"
                                  :min 3
                                  :max 10}))))
  (is (= 202 (:status (post-spec {:name ::good-spec-b
                                  :type "integer"}))))
  (is (= 202 (:status (post-spec {:name ::good-spec-c
                                  :type "list"
                                  :definition [:foo {:type "integer"}
                                               :bar {:type "integer"}
                                               :baz {:type "integer-range"
                                                     :min 3
                                                     :max 10}]})))))

(deftest good-spec-202-with-reference
  (let [schema {:name ::ref-good-spec-a
                :type "integer-range"
                :min 3
                :max 10}
        r1       (post-spec schema)]
    (is-submap {:status 202} r1)
    (if (= 202 (:status r1))
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)]
        (is (= 202 (:status (post-spec {:name ::ref-good-spec-b
                                        :type "id"
                                        :id    id}))))
        (is (= 202 (:status (post-spec {:name ::ref-good-spec-b
                                        :type "list"
                                        :definition [:foo {:type "id"
                                                           :id   id}
                                                     :bar {:type "integer"}
                                                     :baz {:type "integer-range"
                                                           :min 3
                                                           :max 10}]}))))))))

(deftest bad-specs
  (is (= 400 (:status (post-spec {:type "integer"})))   "Missing args (name)")
  (is (= 400 (:status (post-spec {:name :foo
                                  :type "integer"})))   "Unnamespaced name")
  (is (= 400 (:status (post-spec {:name ::foo
                                  :type "foo"})))       "Unknown type")
  (is (= 400 (:status (post-spec {:name ::foo
                                  :type "integer-range"
                                  :foo 1})))            "Invalid type")
  (is (= 400 (:status (post-spec {:name ::foo
                                  :type "list"
                                  :definition [:foo]}))) "Invalid type")

  (is (= 400 (:status (post-spec {:name ::foo
                                  :type "list"
                                  :definition [:foo
                                               {:type "foo"}]}))) "Invalid type"))
