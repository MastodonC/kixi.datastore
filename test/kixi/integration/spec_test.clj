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
  (let [schema {::ss/name ::new-good-spec-a
                ::ss/schema {::ss/type "integer-range"
                             ::ss/min 3
                             ::ss/max 10}
                ::ss/sharing {::ss/read [uid]
                          ::ss/use [uid]}}
        r2 (send-spec uid schema)]
    (when-success r2
      (is-submap (add-ns-to-keys ::ss/_ (dissoc schema ::ss/name ::ss/sharing))
                 (extract-schema r2)))))


(deftest unknown-spec-404
  (let [r-g (get-spec uid "c0bbb46f-9a31-47c2-b30c-62eba45470d4")]
    (not-found r-g)))

(deftest good-spec-202
  (success
   (send {::ss/name ::good-spec-a
          ::ss/schema {::ss/type "integer-range"
                       ::ss/min 3
                       ::ss/max 10}
          ::ss/sharing {::ss/read [uid]
                        ::ss/use [uid]}}))
  (success
   (send {::ss/name ::good-spec-b
          ::ss/schema {::ss/type "integer"}
          ::ss/sharing {::ss/read [uid]
                        ::ss/use [uid]}}))
  (success
   (send {::ss/name ::good-spec-c
          ::ss/schema {::ss/type "list"
                       ::ss/definition [:foo {::ss/type "integer"}
                                        :bar {::ss/type "integer"}
                                        :baz {::ss/type "integer-range"
                                              ::ss/min 3
                                              ::ss/max 10}]}
          ::ss/sharing {::ss/read [uid]
                        ::ss/use [uid]}})))

(deftest good-spec-202-with-reference
  (let [schema {::ss/name ::ref-good-spec-a
                ::ss/schema {::ss/type "integer-range"
                             ::ss/min 3
                             ::ss/max 10}
                ::ss/sharing {::ss/read [uid]
                              ::ss/use [uid]}}
        r1       (send schema)]
    (when-success r1
      (let [id (::ss/id (extract-schema r1))]
        (success
         (send {::ss/name ::ref-good-spec-b
                ::ss/schema {::ss/type "id"
                             ::ss/id    id}
                ::ss/sharing {::ss/read [uid]
                              ::ss/use [uid]}}))
        (success
         (send {::ss/name ::ref-good-spec-b
                ::ss/schema {::ss/type "list"
                             ::ss/definition [:foo {::ss/type "id"
                                                    ::ss/id   id}
                                              :bar {::ss/type "integer"}
                                              :baz {::ss/type "integer-range"
                                                    ::ss/min 3
                                                    ::ss/max 10}]}
                ::ss/sharing {::ss/read [uid]
                              ::ss/use [uid]}}))))))

(defn rejected-schema
  [event]
  (is (contains? event :kixi.comms.event/key) "Is not an event.")
  (is (= :kixi.datastore.schema/rejected (:kixi.comms.event/key event)) "Was not rejected."))

(deftest bad-specs
  (rejected-schema
   (send {::ss/type "integer"}))
  (rejected-schema
   (send {::ss/name :foo
          ::ss/schema {::ss/type "integer"}}))
  (rejected-schema
   (send {::ss/name ::foo
          ::ss/schema {::ss/type "foo"}}))
  (rejected-schema
   (send {::ss/name ::foo
          ::ss/schema {::ss/type "integer-range"
                       :foo 1}}))
  (rejected-schema
   (send {::ss/name ::foo
          ::ss/schema {::ss/type "list"
                       :definition [:foo]}}))

  (rejected-schema
   (send {::ss/name ::foo
          ::ss/schema {::ss/type "list"
                       ::ss/definition [:foo
                                        {::ss/type "foo"}]}})))
