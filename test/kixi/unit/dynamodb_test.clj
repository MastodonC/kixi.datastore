(ns kixi.unit.dynamodb-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.data :as data]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.dynamodb :as db]))

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


(deftest flatten-is-reflected-by-inflate
  (checking ""
            sample-size
            [metadata (s/gen ::md/file-metadata)]
            (is
             (try
               (first-2-nil? (data/diff metadata
                                        (db/inflate-map (db/flatten-map metadata))))
               (catch Exception e
                 (prn "E: " metadata))))))
