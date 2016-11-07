(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [schemastore :as ss]
             [transport-specs :refer [add-ns-to-keys]]]
            [kixi.integration.base :as base :refer [bad-request success when-accepted not-found cycle-system-fixture
                                                    extract-id extract-schema is-submap not-found get-spec-direct
                                                    accepted]]))

(def uuid-regex
  #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

(def uid (base/uuid))

(def post-spec (partial base/post-spec uid))
(def post-spec-and-wait (partial base/post-spec-and-wait uid))
(def get-spec (partial base/get-spec uid))

(def location-regex
  (re-pattern (str #"schema\/" uuid-regex)))

(use-fixtures :once cycle-system-fixture)

(deftest good-round-trip
  (let [schema {:name ::new-good-spec-a
                :schema {:type "integer-range"
                         :min 3
                         :max 10}
                :sharing {:read [uid]
                          :use [uid]}}
        r1 (post-spec schema)]
    (when-accepted r1
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)
            r2       (get-spec id)]
        (is (re-find location-regex location))
        (is (re-find uuid-regex id))
        (success r2)
        (is-submap (add-ns-to-keys ::ss/_ (dissoc schema :name :sharing))
                   (extract-schema r2))))))

(deftest unknown-spec-404
  (let [r-g (get-spec-direct uid "c0bbb46f-9a31-47c2-b30c-62eba45470d4")]
    (not-found r-g)))

(deftest repost-spec-get-same-result
  (let [schema {:name ::reposted-a
                :schema {:type "integer"}
                :sharing {:read [uid]
                          :use [uid]}}
        r-g1 (post-spec-and-wait schema)
        r-g2 (post-spec schema)]
    (when-accepted r-g1
      (when-accepted r-g2
        (is (= (get-in r-g1 [:headers "Location"])
               (get-in r-g2 [:headers "Location"])))))))

(deftest good-spec-202
  (accepted 
   (post-spec {:name ::good-spec-a
               :schema {:type "integer-range"
                        :min 3
                        :max 10}
               :sharing {:read [uid]
                         :use [uid]}}))
  (accepted 
   (post-spec {:name ::good-spec-b
               :schema {:type "integer"}
               :sharing {:read [uid]
                         :use [uid]}}))
  (accepted 
   (post-spec {:name ::good-spec-c
               :schema {:type "list"
                        :definition [:foo {:type "integer"}
                                     :bar {:type "integer"}
                                     :baz {:type "integer-range"
                                           :min 3
                                           :max 10}]}
               :sharing {:read [uid]
                         :use [uid]}})))

(deftest good-spec-202-with-reference
  (let [schema {:name ::ref-good-spec-a
                :schema {:type "integer-range"
                         :min 3
                         :max 10}
                :sharing {:read [uid]
                          :use [uid]}}
        r1       (post-spec-and-wait schema)]
    (when-accepted r1      
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)]
        (accepted 
         (post-spec {:name ::ref-good-spec-b
                     :schema {:type "id"
                              :id    id}
                     :sharing {:read [uid]
                               :use [uid]}}))
        (accepted 
         (post-spec {:name ::ref-good-spec-b
                     :schema {:type "list"
                              :definition [:foo {:type "id"
                                                 :id   id}
                                           :bar {:type "integer"}
                                           :baz {:type "integer-range"
                                                 :min 3
                                                 :max 10}]}
                     :sharing {:read [uid]
                               :use [uid]}}))))))

(deftest bad-specs
  (bad-request 
   (post-spec {:type "integer"}))
  (bad-request 
   (post-spec {:name :foo
               :schema {:type "integer"}}))
  (bad-request 
   (post-spec {:name ::foo
               :schema {:type "foo"}}))
  (bad-request 
   (post-spec {:name ::foo
               :schema {:type "integer-range"
                        :foo 1}}))
  (bad-request 
   (post-spec {:name ::foo
               :schema {:type "list"
                        :definition [:foo]}}))

  (bad-request 
   (post-spec {:name ::foo
               :schema {:type "list"
                        :definition [:foo
                                     {:type "foo"}]}})))
