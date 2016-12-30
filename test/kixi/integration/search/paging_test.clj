(ns kixi.integration.search.paging-test
  (:require [clojure.test :refer :all]
            [clojure.spec.test :refer [with-instrument-disabled]]
            [clojure.core.async :as async]
            [clojure.spec.gen :as gen]
            [environ.core :refer [env]]
            [clj-http.client :as client]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [kixi.datastore
             [metadata-creator :as mdc]
             [metadatastore :as ms]
             [schemastore :as ss]
             [web-server :as ws]]
            [kixi.integration.base :as base :refer :all]))

(alias 'ws 'kixi.datastore.web-server)

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(def sample-size (Integer/valueOf (str (env :generative-testing-size "100"))))
(def such-that-size 100)

(deftest paging
  (let [total-files 56
        uid (uuid)
        metadata (create-metadata uid "./test-resources/metadata-one-valid.csv")
        _ (doseq [i (range total-files)]
            (send-file-and-metadata-no-wait
             metadata))]
    (wait-for-pred
     #(= total-files
         (get-in (search-metadata uid [])
                 [:body :paging :total])))
    (is-submap {:body {:paging {:total total-files}}}
               (search-metadata uid []))

    (checking "various combinations of index and count return correct items"
              sample-size
              [dex (gen/elements (range total-files))
               cnt (gen/elements (range total-files))]
              (let [resp (search-metadata uid [] dex cnt)]
                (is-submap {:body {:paging {:total total-files
                                            :index dex
                                            :count (min (- total-files
                                                           dex) 
                                                        cnt)}}}
                           resp)
                (is (= (min (- total-files
                               dex) 
                            cnt)
                       (count (get-in resp [:body :items]))))))

    (checking "out of bounds index returns nothing"
              sample-size
              [dex (gen/such-that (partial (complement (set (range total-files))))
                                  (gen/int)
                                  such-that-size)]
              (let [resp (search-metadata uid [] dex nil)]
                (if (pos? dex)
                  (is-submap {:body {:items []
                                     :paging {:total total-files
                                              :index dex
                                              :count 0}}}
                             resp)
                  (is-submap {:status 400
                              :body {::ws/error "query-index-invalid"}}
                             resp))))

    (checking "over counting is fine, negative gets 400'd"
              sample-size
              [cnt (gen/such-that (partial (complement (set (range total-files)))) 
                                  (gen/int) 
                                  such-that-size)]
              (let [resp (search-metadata uid [] nil cnt)]
                (if (pos? cnt)
                  (is-submap {:body {:paging {:total total-files
                                              :index 0
                                              :count total-files}}}
                             resp)
                  (is-submap {:status 400
                              :body {::ws/error "query-count-invalid"}}
                             resp))))))
