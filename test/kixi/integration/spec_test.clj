(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [schemastore :as ss]
             [transport-specs :refer [add-ns-to-keys]]]
            [kixi.integration.base :as base :refer :all]))

(def uuid-regex
  #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

(def uid (uuid))

(def send (partial send-spec uid))
(def send-no-wait (partial send-spec-no-wait uid))

(def location-regex
  (re-pattern (str #"schema\/" uuid-regex)))

(use-fixtures :once cycle-system-fixture extract-comms)

(deftest good-round-trip
  (let [schema {:name ::new-good-spec-a
                :schema {:type "integer-range"
                         :min 3
                         :max 10}
                :sharing {:read [uid]
                          :use [uid]}}
        r2 (send-spec uid schema)]
    (when-success r2
      (is-submap (add-ns-to-keys ::ss/_ (dissoc schema :name :sharing))
                 (extract-schema r2)))))


(deftest unknown-spec-404
  (let [r-g (get-spec uid "c0bbb46f-9a31-47c2-b30c-62eba45470d4")]
    (not-found r-g)))

(deftest good-spec-202
  (success
   (send {:name ::good-spec-a
          :schema {:type "integer-range"
                   :min 3
                   :max 10}
          :sharing {:read [uid]
                    :use [uid]}}))
  (success
   (send {:name ::good-spec-b
          :schema {:type "integer"}
          :sharing {:read [uid]
                    :use [uid]}}))
  (success
   (send {:name ::good-spec-c
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
        r1       (send schema)]
    (when-success r1
      (let [id (::ss/id (extract-schema r1))]
        (success
         (send {:name ::ref-good-spec-b
                :schema {:type "id"
                         :id    id}
                :sharing {:read [uid]
                          :use [uid]}}))
        (success
         (send {:name ::ref-good-spec-b
                :schema {:type "list"
                         :definition [:foo {:type "id"
                                            :id   id}
                                      :bar {:type "integer"}
                                      :baz {:type "integer-range"
                                            :min 3
                                            :max 10}]}
                :sharing {:read [uid]
                          :use [uid]}}))))))

(defn rejected-schema
  [event]
  (is (contains? event :kixi.comms.event/key) "Is not an event.")
  (is (= :kixi.datastore.schema/rejected (:kixi.comms.event/key event)) "Was not rejected."))

(deftest bad-specs
  (rejected-schema
   (send {:type "integer"}))
  (rejected-schema
   (send {:name :foo
          :schema {:type "integer"}}))
  (rejected-schema
   (send {:name ::foo
          :schema {:type "foo"}}))
  (rejected-schema
   (send {:name ::foo
          :schema {:type "integer-range"
                   :foo 1}}))
  (rejected-schema
   (send {:name ::foo
          :schema {:type "list"
                   :definition [:foo]}}))

  (rejected-schema
   (send {:name ::foo
          :schema {:type "list"
                   :definition [:foo
                                {:type "foo"}]}})))
