(ns kixi.unit.schemastore.schemastore-test
  (:require [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.validator :as sv]
            [kixi.datastore.schemastore.inmemory :as ssim]
            [kixi.datastore.system :refer [new-system]]
            [kixi.datastore.communications :as c]
            [kixi.integration.base :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all]))

(defonce system (atom nil))
(defn inmemory-schemastore-fixture
  [all-tests]
  (let [kds (select-keys (new-system :local) [:schemastore :communications])]
    (reset! system (component/start-system kds))
    (all-tests)
    (reset! system (component/stop-system @system))))

(use-fixtures :once inmemory-schemastore-fixture)

(defn wait-for-schema-id
  [id schemastore]
  (loop [tries 10]
    (when-not (-> schemastore
                  :data
                  (deref)
                  (get id))
      (Thread/sleep 100)
      (if (zero? (dec tries))
        (throw (Exception. "Schema ID never appeared."))
        (recur (dec tries))))))

(deftest submit-and-validate-schema
  (let [{:keys [communications schemastore]} @system
        id         (uuid)
        schema-req {::c/type :schemastore
                    ::ss/name ::foobar
                    ::ss/schema {::ss/type "list"
                                 ::ss/definition [:foo {::ss/type "integer"}
                                                  :bar {::ss/type "integer"}
                                                  :baz {::ss/type "integer-range"
                                                        ::ss/min 3
                                                        ::ss/max 10}]}
                    ::ss/id id}]
    (c/submit communications schema-req)
    (wait-for-schema-id id schemastore)
    (is (is-submap schema-req (-> schemastore
                                  :data
                                  (deref)
                                  (get id))))
    (is (sv/valid? schemastore id [1 2 5]))
    (is (sv/valid? schemastore id [(+ 1 1) 2 5]))
    (is (sv/valid? schemastore id ["1" "2" "5"]))
    (is (not (sv/valid? schemastore id [1 2 2])))
    (is (not (sv/valid? schemastore id ["1" "2" "2"])))))
