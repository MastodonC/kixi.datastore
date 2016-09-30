(ns kixi.unit.segmentation.inmemory-test
  (:require [clojure.test :refer :all]
            [kixi.datastore.segmentation.inmemory :as seg]))

(deftest segment-minimal-file
  (let [files (seg/segmentate-file-by-column-values "cola" "./test-resources/segmentation/small-segmentable-file.csv")]
    (is (= 3
           (count (keys files))))))

