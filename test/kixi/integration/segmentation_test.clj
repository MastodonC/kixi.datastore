(ns kixi.integration.segmentation-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]
            [kixi.datastore.metadatastore :as ms]))

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'ss 'kixi.datastore.schemastore)
(alias 'seg 'kixi.datastore.segmentation)

(use-fixtures :once cycle-system-fixture)

(def base-segmented-file-name "./test-resources/segmentation/small-segmentable-file-")

(def prn-slurp (comp prn slurp))


(deftest group-rows-by-unknown-file
  (let [sr (post-segmentation (str file-url "/foo/segmentation")
                              {:type "column"
                               :column-name "foo"})]
    (is (= 404
           (:status sr)))))

(deftest group-rows-by-column-small
  (let [pfr (post-file "./test-resources/segmentation/small-segmentable-file.csv")
        base-file-id (extract-id pfr)]
    (is (= 201
           (:status pfr)))
    (let [sr (post-segmentation (str (get-in pfr [:headers "Location"]) "/segmentation")
                                {:type "column"
                                 :column-name "cola"})]
      (is (= 201
             (:status sr)))
      (wait-for-metadata-key base-file-id ::ms/segmentations)
      (let [base-file-meta-resp (get-metadata base-file-id)
            base-file-meta (:body base-file-meta-resp)
            segment-ids (::seg/segment-ids (first (::ms/segmentations base-file-meta)))]
        (is (= 200
               (:status base-file-meta-resp)))
        (is (= 3
               (count segment-ids)))
        (doseq [seg-id segment-ids]
          (let [seg-meta-resp (get-metadata seg-id)]
            (is-submap {:status 200
                        :body {::ss/id (::ss/id base-file-meta)
                               ::ms/type (::ms/type base-file-meta)
                               ::ms/provanance {::ms/parent-id base-file-id}
                               ::ms/size-bytes 27}}
                       seg-meta-resp)
            (is (files-match?
                 (str base-segmented-file-name (get-in seg-meta-resp [:body ::seg/segment ::seg/value]) ".csv")
                 (dload-file-by-id seg-id)))))))))

(deftest group-rows-by-invalid-column
  (let [pfr (post-file "./test-resources/segmentation/small-segmentable-file.csv")
        base-file-id (extract-id pfr)]
    (is (= 201
           (:status pfr)))
    (let [sr (post-segmentation (str (get-in pfr [:headers "Location"]) "/segmentation")
                                {:type "column"
                                 :column-name "foo"})]
      (is (= 201
             (:status sr)))
      (wait-for-metadata-key base-file-id ::ms/segmentations)
      (let [base-file-meta-resp (get-metadata base-file-id)
            base-file-meta (:body base-file-meta-resp)
            segment-ids (::seg/segment-ids (first (::ms/segmentations base-file-meta)))]
        (is-submap
         {:status 200
          :body {::ms/segmentations [{::seg/created false
                                      ::seg/error {::seg/reason "invalid-column"
                                                   ::seg/cause "foo"}}]}}
         base-file-meta-resp)))))


