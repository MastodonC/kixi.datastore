(ns kixi.integration.segmentation-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]
             [segmentation :as seg]]
            [kixi.integration.base :refer :all]))

;; TODO Segmentation needs to be moved over to command pattern so these
;; tests are temporarily disabled.

(comment (alias 'ms 'kixi.datastore.metadatastore)
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

         (use-fixtures :once
           cycle-system-fixture
           extract-comms
           (setup-schema uid small-segmentable-file-schema small-segmentable-file-schema-id))

         (def base-segmented-file-name "./test-resources/segmentation/small-segmentable-file-")

         (def prn-slurp (comp prn slurp))

         (deftest group-rows-by-unknown-file
           (let [sr (post-segmentation (str file-url "/foo/segmentation")
                                       {:type "column"
                                        :column-name "foo"})]
             (not-found sr)))


         (deftest group-rows-by-invalid-column
           (let [pfr (post-file uid
                                "./test-resources/segmentation/small-segmentable-file.csv"
                                @small-segmentable-file-schema-id)
                 base-file-id (extract-id pfr)
                 locat  (get-in pfr [:headers "Location"])]
             (when-created pfr
               (let [sr (post-segmentation (str locat "/segmentation")
                                           {:type "column"
                                            :column-name "bar"})]
                 (when-created sr
                   (wait-for-metadata-key uid base-file-id ::ms/segmentations)
                   (let [base-file-meta-resp (get-metadata uid base-file-id)
                         base-file-meta (:body base-file-meta-resp)
                         segment-ids (::seg/segment-ids (first (::ms/segmentations base-file-meta)))]
                     (is-submap
                      {:status 200
                       :body {::ms/segmentations [{::seg/created false
                                                   ::seg/error {::seg/reason "invalid-column"
                                                                ::seg/cause "bar"}}]}}
                      base-file-meta-resp)))))))

         (deftest group-rows-by-column-small
           (let [pfr (post-file uid
                                "./test-resources/segmentation/small-segmentable-file.csv"
                                @small-segmentable-file-schema-id)
                 base-file-id (extract-id pfr)
                 locat (get-in pfr [:headers "Location"])]
             (when-created pfr
               (let [sr (post-segmentation (str locat "/segmentation")
                                           {:type "column"
                                            :column-name "cola"})]
                 (when-created sr
                   (wait-for-metadata-key uid base-file-id ::ms/segmentations)
                   (let [base-file-meta-resp (get-metadata uid base-file-id)
                         base-file-meta (:body base-file-meta-resp)
                         segment-ids (::seg/segment-ids (first (::ms/segmentations base-file-meta)))]
                     (success base-file-meta-resp)
                     (is (= 3
                            (count segment-ids)))
                     (doseq [seg-id segment-ids]
                       (let [seg-meta-resp (get-metadata uid seg-id)]
                         (is-submap {:status 200
                                     :body {::ms/schema {::ss/id @small-segmentable-file-schema-id}
                                            ::ms/id seg-id
                                            ::ms/type (::ms/type base-file-meta)
                                            ::ms/provenance {::ms/parent-id base-file-id}
                                            ::ms/size-bytes 27}}
                                    seg-meta-resp)
                         (if (= 200 (:status seg-meta-resp))
                           (is (files-match?
                                (str base-segmented-file-name (get-in seg-meta-resp [:body ::seg/segment ::seg/value]) ".csv")
                                (dload-file-by-id uid seg-id))))))))))))
         )
