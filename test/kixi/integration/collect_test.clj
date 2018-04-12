(ns kixi.integration.collect-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [kixi.integration.base :as base :refer :all]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'ms 'kixi.datastore.metadatastore)
(sh/alias 'c 'kixi.datastore.collect)

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(defn random-uuid-set
  ([]
   (random-uuid-set 10))
  ([n]
   (set (repeatedly (inc (rand-int n)) uuid))))

(deftest happy-collect-request
  (let [uid (uuid)
        dr (empty-datapack uid)
        message "happy"
        groups (random-uuid-set)]
    (when-success dr
      (send-collection-request-cmd uid message groups (get-in dr [:body ::ms/id]))
      (let [event (wait-for-events uid :kixi.datastore.collect/collection-requested)]
        (is (= message (::c/message event)))
        (is (= groups (::c/groups event)))
        (is (= (get-in dr [:body ::ms/id]) (::ms/id event)))))))

(deftest collect-request-invalid-cmd
  (let [uid (uuid)
        dr (empty-datapack uid)
        message :not-a-string
        groups (random-uuid-set)]
    (when-success dr
      (with-redefs [clojure.spec.alpha/valid? (constantly true)]
        (send-collection-request-cmd uid message groups (get-in dr [:body ::ms/id])))
      (let [event (wait-for-events uid :kixi.datastore.collect/collection-request-rejected)]
        (is (= :invalid-cmd (:kixi.event.collect.rejection/reason event)))))))

(deftest collect-request-unauthorised
  (let [uid (uuid)
        uid2 (uuid)
        dr (empty-datapack uid)
        message "unauthorised"
        groups (random-uuid-set)]
    (when-success dr
      (with-redefs [clojure.spec.alpha/valid? (constantly true)]
        (send-collection-request-cmd uid2 message groups (get-in dr [:body ::ms/id])))
      (let [event (wait-for-events uid2 :kixi.datastore.collect/collection-request-rejected)]
        (is (= :unauthorised (:kixi.event.collect.rejection/reason event)))))))

(deftest collect-request-incorrect-type
  (let [uid (uuid)
        dr (send-file-and-metadata
            (create-metadata
             uid
             "./test-resources/metadata-one-valid.csv"))
        message "unauthorised"
        groups (random-uuid-set)]
    (when-success dr
      (with-redefs [clojure.spec.alpha/valid? (constantly true)]
        (send-collection-request-cmd uid message groups (get-in dr [:body ::ms/id])))
      (let [event (wait-for-events uid :kixi.datastore.collect/collection-request-rejected)]
        (is (= :incorrect-type (:kixi.event.collect.rejection/reason event)))))))
