(ns kixi.integration.search.paging-test
  (:require [clojure.test :refer :all]
            [clojure.spec.test :refer [with-instrument-disabled]]
            [clojure.core.async :as async]
            [clj-http.client :as client]
            [kixi.datastore
             [metadata-creator :as mdc]
             [metadatastore :as ms]
             [schemastore :as ss]
             [web-server :as ws]]
            [kixi.integration.base :as base :refer :all]))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest paging)
