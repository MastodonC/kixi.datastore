(ns kixi.integration.search.filter-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [clojure.math.combinatorics :as combo :refer [subsets]]
            [kixi.datastore
             [metadatastore :as ms]
             [web-server :as ws]]
            [kixi.integration.base :as base :refer :all :exclude [create-metadata]]))

(alias 'ms 'kixi.datastore.metadatastore)

(defn create-metadata
  [uid]
  (base/create-metadata uid "./test-resources/metadata-one-valid.csv"))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest only-returns-csvs
  (let [total-files 10
        uid (uuid)
        metadata (create-metadata uid)]
    (->> (range total-files)
         (map (fn [i]
                (send-file-and-metadata-no-wait (if (odd? i)
                                                  (assoc metadata ::ms/file-type "pdf")
                                                  metadata))))
         doall
         (map ::ms/id)
         (map #(wait-for-metadata-key uid % ::ms/id))
         doall)
    (is-submap {:paging {:total total-files
                         :count total-files
                         :index 0}}
               (:body
                (search-metadata uid [::ms/file-read])))
    (is-submap {:paging {:total (/ total-files 2)
                         :count (/ total-files 2)
                         :index 0}}
               (:body
                (search-metadata uid [::ms/file-read] nil nil nil [::ms/file-type "csv"])))))
