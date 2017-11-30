(ns kixi.unit.metadatastore.command-handler
  (:require [kixi.datastore.metadatastore.command-handler :as sut]
            [kixi.datastore.metadatastore :as ms]
            [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [clojure.spec.test.alpha :as stest]
            [clojure.test.check :as tc]))

(def sample-size 100)

(defn check
  [sym]
  (-> sym
      (stest/check {:clojure.spec.test.alpha.check/opts {:num-tests sample-size}})
      first
      stest/abbrev-result
      :failure))

(def types    (set (map first  (keys (methods kixi.comms/command-payload)))))
(def versions (set (map second (keys (methods kixi.comms/command-payload)))))

(s/def :kixi.command/type types)
(s/def :kixi.command/version versions)

(deftest check-create-delete-file-handler-inner
  (stest/instrument [`ms/authorised-fn
                     `ms/retrieve-fn]
                    {:stub #{`ms/authorised-fn
                             `ms/retrieve-fn}})
  (is (nil?
       (check `sut/create-delete-file-handler-inner))))
