(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clj-http.client :as client]
            [kixi.datastore.transport-specs :refer [add-ns-to-keys]]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.integration.base :refer [service-url cycle-system-fixture uuid
                                           post-spec get-spec extract-schema parse-json
                                           wait-for-url is-submap extract-id
                                           get-spec-direct when-accepted post-spec-and-wait]]))

(def uuid-regex
  #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

(def uid (uuid))

(def location-regex
  (re-pattern (str #"schema\/" uuid-regex)))

(use-fixtures :once cycle-system-fixture)

(deftest good-round-trip
  (let [schema {:name ::new-good-spec-a
                :schema {:type "integer-range"
                         :min 3
                         :max 10}}
        r1 (post-spec schema uid)]
    (when-accepted r1
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)
            r2       (get-spec id uid)]
        (is (re-find location-regex location))
        (is (re-find uuid-regex id))
        (is-submap {:status 200} r2)
        (is-submap (add-ns-to-keys ::ss/_ (dissoc schema :name))
                   (extract-schema r2))))))

(deftest unknown-spec-404
  (let [r-g (get-spec-direct "c0bbb46f-9a31-47c2-b30c-62eba45470d4")]
    (is (= 404
           (:status r-g)))))

(comment "This next test is likely to be flaky as it depends on how quickly the events are consumed. Need an idempotent schema create...")
(deftest repost-spec-get-same-result
  (let [schema {:name ::reposted-a
                :schema {:type "integer"}}
        r-g1 (post-spec-and-wait schema uid)
        r-g2 (post-spec schema uid)]
    (when-accepted r-g1
      (when-accepted r-g2
        (is (= (get-in r-g1 [:headers "Location"])
               (get-in r-g2 [:headers "Location"])))))))

(deftest good-spec-202
  (is (= 202 (:status (post-spec {:name ::good-spec-a
                                  :schema {:type "integer-range"
                                           :min 3
                                           :max 10}} uid))))
  (is (= 202 (:status (post-spec {:name ::good-spec-b
                                  :schema {:type "integer"}} uid))))
  (when-accepted (post-spec {:name ::good-spec-c
                             :schema {:type "list"
                                      :definition [:foo {:type "integer"}
                                                   :bar {:type "integer"}
                                                   :baz {:type "integer-range"
                                                         :min 3
                                                         :max 10}]}} uid)
    true))

(deftest good-spec-202-with-reference
  (let [schema {:name ::ref-good-spec-a
                :schema {:type "integer-range"
                         :min 3
                         :max 10}}
        r1       (post-spec-and-wait schema uid)]
    (when-accepted r1      
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)]
        (is (= 202 (:status (post-spec {:name ::ref-good-spec-b
                                        :schema {:type "id"
                                                 :id    id}} uid))))
        (is (= 202 (:status (post-spec {:name ::ref-good-spec-b
                                        :schema {:type "list"
                                                 :definition [:foo {:type "id"
                                                                    :id   id}
                                                              :bar {:type "integer"}
                                                              :baz {:type "integer-range"
                                                                    :min 3
                                                                    :max 10}]}} uid))))))))

(deftest bad-specs
  (is (= 400 (:status (post-spec {:type "integer"} uid)))   "Missing args (name)")
  (is (= 400 (:status (post-spec {:name :foo
                                  :schema {:type "integer"}} uid)))   "Unnamespaced name")
  (is (= 400 (:status (post-spec {:name ::foo
                                  :schema {:type "foo"}} uid)))       "Unknown type")
  (is (= 400 (:status (post-spec {:name ::foo
                                  :schema {:type "integer-range"
                                           :foo 1}} uid)))            "Invalid type")
  (is (= 400 (:status (post-spec {:name ::foo
                                  :schema {:type "list"
                                           :definition [:foo]}} uid))) "Invalid type")

  (is (= 400 (:status (post-spec {:name ::foo
                                  :schema {:type "list"
                                           :definition [:foo
                                                        {:type "foo"}]}} uid))) "Invalid type"))
