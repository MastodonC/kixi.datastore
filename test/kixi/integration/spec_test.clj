(ns kixi.integration.spec-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [schemastore :as ss]
             [transport-specs :refer [add-ns-to-keys]]]
            [kixi.integration.base :as base :refer :all]))

(def uuid-regex
  #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

(def location-regex
  (re-pattern (str #"schema\/" uuid-regex)))

(use-fixtures :once cycle-system-fixture extract-comms)

(deftest good-round-trip
  (let [uid (uuid)
        schema {::ss/name ::new-good-spec-a
                ::ss/provenance {::ss/source "upload"
                                 :kixi.user/id uid}
                ::ss/schema {::ss/type "integer-range"
                             ::ss/min 3
                             ::ss/max 10}
                ::ss/sharing {::ss/read [uid]
                              ::ss/use [uid]}}
        r2 (send-spec uid schema)]
    (when-success r2
      (is-submap (add-ns-to-keys ::ss/_ (dissoc schema ::ss/name ::ss/sharing ::ss/provenance))
                 (extract-schema r2)))))


(deftest unknown-spec-404
  (let [uid (uuid)
        r-g (get-spec uid "c0bbb46f-9a31-47c2-b30c-62eba45470d4")]
    (not-found r-g)))

(deftest good-spec-202
  (let [uid (uuid)]
    (success
     (send-spec uid 
                {::ss/name ::good-spec-a
                 ::ss/provenance {::ss/source "upload"
                                  :kixi.user/id uid}
                 ::ss/schema {::ss/type "integer-range"
                              ::ss/min 3
                              ::ss/max 10}
                 ::ss/sharing {::ss/read [uid]
                               ::ss/use [uid]}}))
    (success
     (send-spec uid
                {::ss/name ::good-spec-b
                 ::ss/provenance {::ss/source "upload"
                                  :kixi.user/id uid}
                 ::ss/schema {::ss/type "integer"}
                 ::ss/sharing {::ss/read [uid]
                               ::ss/use [uid]}}))
    (success
     (send-spec uid
                {::ss/name ::good-spec-c
                 ::ss/provenance {::ss/source "upload"
                                  :kixi.user/id uid}
                 ::ss/schema {::ss/type "list"
                              ::ss/definition [:foo {::ss/type "integer"}
                                               :bar {::ss/type "integer"}
                                               :baz {::ss/type "integer-range"
                                                     ::ss/min 3
                                                     ::ss/max 10}]}
                 ::ss/sharing {::ss/read [uid]
                               ::ss/use [uid]}}))))

(deftest good-spec-202-with-reference
  (let [uid (uuid)
        schema {::ss/name ::ref-good-spec-a
                ::ss/provenance {::ss/source "upload"
                                 :kixi.user/id uid}
                ::ss/schema {::ss/type "integer-range"
                             ::ss/min 3
                             ::ss/max 10}
                ::ss/sharing {::ss/read [uid]
                              ::ss/use [uid]}}
        r1 (send-spec uid schema)]
    (when-success r1
      (let [id (::ss/id (extract-schema r1))]
        (success
         (send-spec uid
                    {::ss/name ::ref-good-spec-b
                     ::ss/provenance {::ss/source "upload"
                                      :kixi.user/id uid}
                     ::ss/schema {::ss/type "id"
                                  ::ss/id    id}
                     ::ss/sharing {::ss/read [uid]
                                   ::ss/use [uid]}}))
        (success
         (send-spec uid
                    {::ss/name ::ref-good-spec-b
                     ::ss/provenance {::ss/source "upload"
                                      :kixi.user/id uid}
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
  (is (contains? event :kixi.comms.event/key) (str "Is not an event: " event))
  (is (= :kixi.datastore.schema/rejected (:kixi.comms.event/key event)) (str "Was not rejected: " event)))

(deftest bad-specs
  (let [uid (uuid)]
    (rejected-schema
     (send-spec uid
                {::ss/type "integer"
                 ::ss/provenance {::ss/source "upload"
                                  :kixi.user/id uid}}))
    (rejected-schema
     (send-spec uid
           {::ss/name :foo
            ::ss/provenance {::ss/source "upload"
                             :kixi.user/id uid}
            ::ss/schema {::ss/type "integer"}}))
    (rejected-schema
     (send-spec uid
           {::ss/name ::foo
            ::ss/provenance {::ss/source "upload"
                             :kixi.user/id uid}
            ::ss/schema {::ss/type "foo"}}))
    (rejected-schema
     (send-spec uid
           {::ss/name ::foo
            ::ss/provenance {::ss/source "upload"
                             :kixi.user/id uid}
            ::ss/schema {::ss/type "integer-range"
                         :foo 1}}))
    (rejected-schema
     (send-spec uid
           {::ss/name ::foo
            ::ss/provenance {::ss/source "upload"
                             :kixi.user/id uid}
            ::ss/schema {::ss/type "list"
                         :definition [:foo]}}))

    (rejected-schema
     (send-spec uid
           {::ss/name ::foo
            ::ss/provenance {::ss/source "upload"
                             :kixi.user/id uid}
            ::ss/schema {::ss/type "list"
                         ::ss/definition [:foo
                                          {::ss/type "foo"}]}}))))
