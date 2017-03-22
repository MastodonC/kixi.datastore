(ns kixi.unit.dynamodb-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.data :as data]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.schemastore :as ss]
            [taoensso
             [timbre :as timbre :refer [error]]]))

(def sample-size (Integer/valueOf (str (env :generative-testing-size "100"))))

(defn first-2-nil?
  [v]
  (and
   (nil? (first v))
   (nil? (second v))))

(db/flatten-map {:type "stored",
                 :file-type "",
                 ::md/id
                 "d2fd0c3b-c35f-4416-b89e-ae20a7f89e7a",
                 :name "000",
                 :provenance
                 {:kixi.datastore.metadatastore/source
                  "upload",
                  :kixi.user/id
                  "3ce35c81-2e66-477e-846a-8cc1e091ab2a",
                  :kixi.datastore.metadatastore/created
                  "2017-03-17T14:30:00.965Z"},
                 :size-bytes 0,
                 :sharing {}})

(defmacro is-match
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           [only-in-ex# only-in-ac# shared#] (clojure.data/diff exp# act#)]
       (cond
         only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing expected elements.")
                                  :expected only-in-ex# :actual act#})
         only-in-ac#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing actual elements.")
                                  :expected only-in-ac# :actual act#})
         :else (clojure.test/do-report {:type :pass
                                        :message "Matched"
                                        :expected exp# :actual act#})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception diffing"
                                :expected ~expected :actual t#}))))


(deftest flatten-is-reflected-by-inflate-for-metadata
  (checking ""
            sample-size
            [metadata (s/gen ::md/file-metadata)]
            (is-match metadata
                      (db/inflate-map (db/flatten-map metadata)))))

(deftest flatten-is-reflected-by-inflate-for-schemas
  (checking ""
            sample-size
            [schema (s/gen ::ss/stored-schema)]
            (is-match schema
                      (db/inflate-map (db/flatten-map schema)))))
