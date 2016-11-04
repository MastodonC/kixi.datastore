(ns kixi.integration.segmentation-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.integration.base :refer :all]
            [kixi.datastore.metadatastore :as ms]))

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'ss 'kixi.datastore.schemastore)
(alias 'seg 'kixi.datastore.segmentation)

(def uid (uuid))

(def small-segmentable-file-schema-id (atom nil))
(def small-segmentable-file-schema {:name ::small-segmentable-file-schema
                                    :schema {:type "list"
                                             :definition [:cola {:type "integer"}
                                                          :colb {:type "integer"}
                                                          :colc {:type "integer"}]}
                                    :sharing {:read [uid]
                                              :use [uid]}})


(defn setup-schema
  [all-tests]
  (let [r (post-spec-and-wait small-segmentable-file-schema uid)]
    (if (= 202 (:status r))
      (reset! small-segmentable-file-schema-id (extract-id r))
      (throw (Exception. "Couldn't post small-segmentable-file-schema"))))
  (all-tests))

(use-fixtures :once cycle-system-fixture setup-schema)

(def base-segmented-file-name "./test-resources/segmentation/small-segmentable-file-")

(def prn-slurp (comp prn slurp))


(deftest group-rows-by-unknown-file
  (let [sr (post-segmentation (str file-url "/foo/segmentation")
                              {:type "column"
                               :column-name "foo"})]
    (not-found sr)))


(deftest group-rows-by-invalid-column
  (let [pfr (post-file "./test-resources/segmentation/small-segmentable-file.csv"
                       @small-segmentable-file-schema-id
                       uid)
        base-file-id (extract-id pfr)
        locat  (get-in pfr [:headers "Location"])]
    (when-created pfr
      (let [sr (post-segmentation (str locat "/segmentation")
                                  {:type "column"
                                   :column-name "bar"})]
        (when-created sr
          (wait-for-metadata-key base-file-id ::ms/segmentations uid)
          (let [base-file-meta-resp (get-metadata base-file-id uid)
                base-file-meta (:body base-file-meta-resp)
                segment-ids (::seg/segment-ids (first (::ms/segmentations base-file-meta)))]
            (is-submap
             {:status 200
              :body {::ms/segmentations [{::seg/created false
                                          ::seg/error {::seg/reason "invalid-column"
                                                       ::seg/cause "bar"}}]}}
             base-file-meta-resp)))))))

(deftest group-rows-by-column-small
  (let [pfr (post-file "./test-resources/segmentation/small-segmentable-file.csv"
                       @small-segmentable-file-schema-id
                       uid)
        base-file-id (extract-id pfr)
        locat (get-in pfr [:headers "Location"])]
    (when-created pfr      
      (let [sr (post-segmentation (str locat "/segmentation")
                                  {:type "column"
                                   :column-name "cola"})]
        (when-created sr
          (wait-for-metadata-key base-file-id ::ms/segmentations uid)
          (let [base-file-meta-resp (get-metadata base-file-id uid)
                base-file-meta (:body base-file-meta-resp)
                segment-ids (::seg/segment-ids (first (::ms/segmentations base-file-meta)))]
            (success base-file-meta-resp)
            (is (= 3
                   (count segment-ids)))
            (doseq [seg-id segment-ids]
              (let [seg-meta-resp (get-metadata seg-id uid)]
                (is-submap {:status 200
                            :body {::ss/id @small-segmentable-file-schema-id
                                   ::ms/type (::ms/type base-file-meta)
                                   ::ms/provenance {::ms/parent-id base-file-id}
                                   ::ms/size-bytes 27}}
                           seg-meta-resp)
                (if (= 200 (:status seg-meta-resp))
                  (is (files-match?
                       (str base-segmented-file-name (get-in seg-meta-resp [:body ::seg/segment ::seg/value]) ".csv")
                       (dload-file-by-id seg-id uid))))))))))))
