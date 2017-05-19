(ns kixi.integration.search.paging-test
  (:require [clojure.spec.gen :as gen]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore
             [metadatastore :as ms]
             [web-server :as ws]]
            [kixi.integration.base :as base :refer :all]))

(alias 'ws 'kixi.datastore.web-server)

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(def sample-size (Integer/valueOf (str (env :generative-testing-size "10"))))
(def such-that-size 100)

(deftest paging
  (let [total-files 56
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

    (checking "various combinations of index and count return correct items"
              sample-size
              [dex (gen/elements (range total-files))
               cnt (gen/elements (range total-files))]
              (let [resp (search-metadata uid [::ms/meta-read] dex cnt)]
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
              (let [resp (search-metadata uid [::ms/meta-read] dex)]
                (if (pos? dex)
                  (is-submap {:body {:items []
                                     :paging {:total total-files
                                              :index dex
                                              :count 0}}}
                             resp)
                  (is-submap {:status 400
                              :body {::ws/error :query-index-invalid}}
                             resp))))

    (checking "over counting is fine, negative gets 400'd"
              sample-size
              [cnt (gen/such-that (partial (complement (set (range total-files))))
                                  (gen/int)
                                  such-that-size)]
              (let [resp (search-metadata uid [::ms/meta-read] nil cnt)]
                (if (pos? cnt)
                  (is-submap {:body {:paging {:total total-files
                                              :index 0
                                              :count total-files}}}
                             resp)
                  (is-submap {:status 400
                              :body {::ws/error :query-count-invalid}}
                             resp))))))
