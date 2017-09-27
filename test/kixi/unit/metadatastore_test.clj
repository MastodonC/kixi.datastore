(ns kixi.unit.metadatastore-test
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.data :as data]
            [kixi.datastore.schemastore.utils :as sh]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as md]
            [taoensso
             [timbre :as timbre :refer [error]]]))

(deftest name-check
  (let [r (keep #(when-not (s/valid? ::md/name %) %) (gen/sample (s/gen ::md/name) 1000))]
    (is (empty? r) (pr-str r)))
  (is (not (s/valid? ::md/name "")))
  (is (not (s/valid? ::md/name "$")))
  (is (s/valid? ::md/name "1"))
  (is (s/valid? ::md/name "Z")))
