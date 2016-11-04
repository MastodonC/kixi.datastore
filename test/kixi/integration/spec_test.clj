(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clj-http.client :as client]
            [kixi.datastore.transport-specs :refer [add-ns-to-keys]]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.integration.base :refer :all]))

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
                         :max 10}
                :sharing {:read [uid]
                          :use [uid]}}
        r1 (post-spec schema uid)]
    (when-accepted r1
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)
            r2       (get-spec id uid)]
        (is (re-find location-regex location))
        (is (re-find uuid-regex id))
        (success r2)
        (is-submap (add-ns-to-keys ::ss/_ (dissoc schema :name :sharing))
                   (extract-schema r2))))))

(deftest unknown-spec-404
  (let [r-g (get-spec-direct "c0bbb46f-9a31-47c2-b30c-62eba45470d4" uid)]
    (not-found r-g)))

(comment "Totally binning this for a bit. Make a specific comment about it"
        (deftest repost-spec-get-same-result
          (let [schema {:name ::reposted-a
                        :schema {:type "integer"}
                        :sharing {:read [uid]
                                  :use [uid]}}
                r-g1 (post-spec-and-wait schema uid)
                r-g2 (post-spec schema uid)]
            (when-accepted r-g1
              (when-accepted r-g2
                (is (= (get-in r-g1 [:headers "Location"])
                       (get-in r-g2 [:headers "Location"]))))))))

(deftest good-spec-202
  (accepted (post-spec {:name ::good-spec-a
                        :schema {:type "integer-range"
                                 :min 3
                                 :max 10}
                        :sharing {:read [uid]
                                  :use [uid]}} uid))
  (accepted (post-spec {:name ::good-spec-b
                        :schema {:type "integer"}
                        :sharing {:read [uid]
                                  :use [uid]}} uid))
  (accepted (post-spec {:name ::good-spec-c
                        :schema {:type "list"
                                 :definition [:foo {:type "integer"}
                                              :bar {:type "integer"}
                                              :baz {:type "integer-range"
                                                    :min 3
                                                    :max 10}]}
                        :sharing {:read [uid]
                                  :use [uid]}} uid)))

(deftest good-spec-202-with-reference
  (let [schema {:name ::ref-good-spec-a
                :schema {:type "integer-range"
                         :min 3
                         :max 10}
                :sharing {:read [uid]
                          :use [uid]}}
        r1       (post-spec-and-wait schema uid)]
    (when-accepted r1      
      (let [location (get-in r1 [:headers "Location"])
            id       (extract-id r1)]
        (accepted (post-spec {:name ::ref-good-spec-b
                              :schema {:type "id"
                                       :id    id}
                              :sharing {:read [uid]
                                        :use [uid]}} uid))
        (accepted (post-spec {:name ::ref-good-spec-b
                              :schema {:type "list"
                                       :definition [:foo {:type "id"
                                                          :id   id}
                                                    :bar {:type "integer"}
                                                    :baz {:type "integer-range"
                                                          :min 3
                                                          :max 10}]}
                              :sharing {:read [uid]
                                        :use [uid]}} uid))))))

(deftest bad-specs
  (bad-request (post-spec {:type "integer"} uid))
  (bad-request (post-spec {:name :foo
                           :schema {:type "integer"}} uid))
  (bad-request (post-spec {:name ::foo
                           :schema {:type "foo"}} uid))
  (bad-request (post-spec {:name ::foo
                           :schema {:type "integer-range"
                                    :foo 1}} uid))
  (bad-request (post-spec {:name ::foo
                           :schema {:type "list"
                                    :definition [:foo]}} uid))

  (bad-request (post-spec {:name ::foo
                           :schema {:type "list"
                                    :definition [:foo
                                                 {:type "foo"}]}} uid)))
