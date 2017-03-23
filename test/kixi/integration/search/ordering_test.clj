(ns kixi.integration.search.ordering-test
  (:require [clojure.test :refer :all]
            [clojure.spec.test :refer [with-instrument-disabled]]
            [clojure.core.async :as async]
            [clojure.spec.gen :as gen]
            [environ.core :refer [env]]
            [clj-http.client :as client]
            [clj-time.coerce :as tc]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [kixi.datastore
             [metadata-creator :as mdc]
             [metadatastore :as ms]
             [schemastore :as ss]
             [web-server :as ws]]
            [kixi.integration.base :as base :refer :all]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.time :as t]))

(alias 'ws 'kixi.datastore.web-server)

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest ordering
  "ordering desc is reverse of asc"
  (let [total-files 20
        uid (uuid)
        metadata (create-metadata uid "./test-resources/metadata-one-valid.csv")
        _ (->> (range total-files)
               (map (fn [_] 
                      (send-file-and-metadata-no-wait
                       metadata)))
               doall
               (map ::ms/id)
               (map #(wait-for-metadata-key uid % ::ms/id))
               doall)]
    (wait-for-pred
     #(= total-files
         (get-in (search-metadata uid [::ms/meta-read])
                 [:body :paging :total])))

    (is-submap {:body {:paging {:total total-files}}}
               (search-metadata uid [::ms/meta-read]))

    (let [asc-resp (search-metadata uid [::ms/meta-read] 0 total-files "asc")
          desc-resp (search-metadata uid [::ms/meta-read] 0 total-files "desc")
          timestamp #(get-in % [::ms/provenance ::ms/created])]
      (success asc-resp)
      (success desc-resp)
      (let [ts-asc (map timestamp (get-in asc-resp [:body :items]))
            ts-desc (map timestamp (get-in desc-resp [:body :items]))]
        (is (apply < (map tc/to-long (map t/from-str ts-asc))))
        (is (apply > (map tc/to-long (map t/from-str ts-desc))))          
        (is (= ts-asc
               (reverse ts-desc)))))))

