(ns kixi.integration.segmentation-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]))

(use-fixtures :once cycle-system-fixture)

(def base-segmented-file-name "./test-resources/segmentation/small-segmentable-file-")

(def prn-slurp (comp prn slurp))

(deftest segment-minimal-file
  (let [pfr (post-file "./test-resources/segmentation/small-segmentable-file.csv")
        base-file-id (extract-id pfr)]
    (is (= 201
           (:status pfr)))
    (let [sr (post-segmentation (str (get-in pfr [:headers "Location"]) "/segmentation")
                                {:type "column"
                                 :column-name "cola"})]
      (is (= 201
             (:status sr)))
      (wait-for-metadata-key base-file-id :segmentation)
      (let [sgr (get-metadata base-file-id)
            segment-ids (:segment-ids (first (:segmentation (:body sgr))))]
        (is (= 200
               (:status sgr)))
        (is (= 3
               (count segment-ids)))
        (doseq [seg-id segment-ids]
          (let [seg-meta-resp (get-metadata seg-id)
                seg-meta (:body seg-meta-resp)
                _ (prn seg-meta)]
            (is (= 200
                   (:status seg-meta-resp)))
            (is (= base-file-id
                   (get-in seg-meta [:segment :source-file-id])))
            (is (files-match?                  
                 (str base-segmented-file-name (get-in seg-meta [:segment :value]) ".csv")
                 (dload-file-by-id (:id seg-meta))))))))))
