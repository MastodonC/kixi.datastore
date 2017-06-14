(ns kixi.unit.dynamodb-test
  (:require [clojure.test :refer :all]
            [clojure.spec :as s]
            [clojure.spec.gen :as gen]
            [clojure.data :as data]
            [com.gfredericks.schpec :as sh]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.metadatastore.license :as l]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.schemastore :as ss]
            [taoensso
             [timbre :as timbre :refer [error]]]))

(def sample-size (Integer/valueOf (str (env :generative-testing-size "100"))))


(defmacro is-submap
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           [only-in-ex# only-in-ac# shared#] (clojure.data/diff exp# act#)]
       (if only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing expected elements.")
                                  :expected only-in-ex# :actual act#})
         (clojure.test/do-report {:type :pass
                                  :message "Matched"
                                  :expected exp# :actual act#})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception diffing"
                                :expected nil :actual t#}))))

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
           [only-in-ac# only-in-ex# shared#] (clojure.data/diff act# exp#)]
       (cond
         only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing expected elements.")
                                  :expected only-in-ex# :actual act#})
         only-in-ac#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Has extra elements.")
                                  :expected {} :actual only-in-ac#})
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

(sh/alias 'lu 'kixi.datastore.metadatastore.license.update)
(sh/alias 'mdu 'kixi.datastore.metadatastore.update)

(deftest vectorise-metadata-path
  (is (= [[::md/name] :set "name" :aa]
         (db/vectorise-metadata-path [::md/name :set "name" :aa])))
  (is (= [[::l/license ::l/usage] :set "license usage" :aa]
         (db/vectorise-metadata-path [::l/license ::l/usage :set "license usage" :aa]))))

(deftest remove-update-from-metadata-path
  (is (= [[::l/license ::l/usage] :set "license usage" :aa]
         (db/remove-update-from-metadata-path [[::lu/license ::lu/usage] 
                                               :set "license usage" :aa]))))

(deftest update-expr-creation
  (is (= "SET #kixidatastoremetadatastore_name = :aa, #kixidatastoremetadatastore_description = :ab, #kixidatastoremetadatastorelicense_licensekixidatastoremetadatastorelicense_usage = :ae ADD #kixidatastoremetadatastore_tags :ac DELETE #kixidatastoremetadatastore_tags :ad"
         (db/concat-update-expr {:set ["#kixidatastoremetadatastore_name = :aa" 
                                       "#kixidatastoremetadatastore_description = :ab" 
                                       "#kixidatastoremetadatastorelicense_licensekixidatastoremetadatastorelicense_usage = :ae"]
                                 :conj ["#kixidatastoremetadatastore_tags :ac"]
                                 :disj ["#kixidatastoremetadatastore_tags :ad"]}))))

(deftest update-data-map-transformed-into-update-expr-map
  (let [test-data {::mdu/name {:set "name"}
                   ::mdu/description {:set "description"}
                   ::mdu/tags {:conj #{"add"} 
                              :disj #{"remove"}}
                   ::lu/license {::lu/usage {:set "license usage"}}}]
    (is-submap {:update-expr
                "SET #kixidatastoremetadatastore_name = :aa, #kixidatastoremetadatastore_description = :ab, #kixidatastoremetadatastorelicense_licensekixidatastoremetadatastorelicense_usage = :ae ADD #kixidatastoremetadatastore_tags :ac DELETE #kixidatastoremetadatastore_tags :ad"
                :expr-attr-names
                {"#kixidatastoremetadatastore_name"
                 "kixi.datastore.metadatastore_name",
                 "#kixidatastoremetadatastore_description"
                 "kixi.datastore.metadatastore_description",
                 "#kixidatastoremetadatastore_tags"
                 "kixi.datastore.metadatastore_tags",
                 "#kixidatastoremetadatastorelicense_licensekixidatastoremetadatastorelicense_usage"
                 "kixi.datastore.metadatastore.license_license|kixi.datastore.metadatastore.license_usage"},
                :expr-attr-vals
                {":aa" "name"
                 ":ab" "description"
                 ":ae" "license usage"
                 ":ac" #{"add"}
                 ":ad" #{"remove"}}}
               (db/update-data-map->dynamo-update test-data))))



