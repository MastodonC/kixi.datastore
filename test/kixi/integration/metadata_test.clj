(ns kixi.integration.metadata-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]
            [kixi.datastore.metadatastore :as ms]))

(alias 'ms 'kixi.datastore.metadatastore)

(use-fixtures :once cycle-system-fixture)

(deftest unknown-file-404
    (let [sr (get-metadata "foo")]
    (is (= 404
           (:status sr)))))

(deftest small-file
  (let [pfr (post-file "./test-resources/segmentation/small-segmentable-file.csv")
        metadata-response (get-metadata (extract-id pfr))]
    (is-submap
     {:status 201}
     pfr)
    (is-submap
     {:status 200
      :body {::ms/id (extract-id pfr)
             ; need to add schema id
             ::ms/type "csv",
             ::ms/name "foo",
             ::ms/size-bytes 50,
             ::ms/provenance {::ms/source "upload"
                              ::ms/pieces-count nil}
             :structural-validation {:valid true}}}
    metadata-response)))

