(ns kixi.integration.datapack-update-test
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]]
            [kixi.integration.base :as base :refer :all]            
            [medley.core :refer [dissoc-in]]))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(defn small-file-into-datapack
  ([uid]
   (small-file-into-datapack uid {}))
  ([uid extra-dp-meta]
   (let [metadata-response (send-file-and-metadata
                            (create-metadata
                             uid
                             "./test-resources/metadata-one-valid.csv"))]
     (when-success metadata-response
       (let [datapack-resp (send-datapack (merge (create-datapack uid uid "small-file-into-a-datapack" #{(extract-id metadata-response)})
                                                 extra-dp-meta))]
         datapack-resp)))))

(deftest datapack-edit-invalid-field
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [meta-id (get-in datapack-resp [:body ::ms/id])
            new-group (uuid)
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/file-type {:set "Invalid"}})]
        (when-event-key event :kixi.datastore.metadatastore/update-rejected
                        (is (= :invalid
                               (get-in event [:kixi.comms.event/payload :reason]))))))))

(deftest datapack-edit-field
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [meta-id (get-in datapack-resp [:body ::ms/id])
            new-group (uuid)
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/description {:set "New Description"}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (get-in metadata [:body ::ms/description])))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= "New Description"
                                 (get-in updated-metadata [:body ::ms/description])))))))))

(deftest datapack-edit-multi-field
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid {::ms/tags #{"orig-one" "orig-two"}})]
    (when-success datapack-resp
      (let [meta-id (get-in datapack-resp [:body ::ms/id])
            new-group (uuid)
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/description {:set "New Description"}
                    :kixi.datastore.metadatastore.update/source {:set "New Source"}
                    :kixi.datastore.metadatastore.update/tags {:conj #{"tag" "tag two"}
                                                               :disj #{"orig-one"}}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (get-in metadata [:body ::ms/description])))
                        (let [updated-metadata (get-metadata uid meta-id)]
                       
                          (is (= "New Description"
                                 (get-in updated-metadata [:body ::ms/description])))
                          (is (= "New Source"
                                 (get-in updated-metadata [:body ::ms/source])))
                          (is (= #{"tag" "tag two" "orig-two"}
                                 (get-in updated-metadata [:body ::ms/tags])))))))))

(comment
  (deftest unreadable-files-arent-removed-when-file-is-added
    (let [uid-one (uuid)
          uid-two (uuid)
          file-one (send-file-and-metadata
                    (create-metadata
                     uid-one
                     "./test-resources/metadata-one-valid.csv"))
          file-two (send-file-and-metadata
                    (create-metadata
                     uid-two
                     "./test-resources/metadata-one-valid.csv"))
          file-one-id (extract-id file-one)
          file-two-id (extract-id file-two)
          pack (send-datapack uid-one [uid-one uid-two] "small-file-into-a-datapack" #{file-one-id})
          pack-id (get-in pack [:body ::ms/id])
          event (update-metadata
                 uid-two pack-id
                 {:kixi.datastore.metadatastore.update/packed-ids {:conj #{file-two-id}}
                  :kixi.datastore.metadatastore.update/tags {:conj #{"New Tag"}}
                  :kixi.datastore.metadatastore.update/description {:set "Added"}})]
      (when-event-key event :kixi.datastore.file-metadata/updated
                      (wait-for-pred #(let [metadata (get-metadata uid-one pack-id)]
                                        (get-in metadata [:body ::ms/description])))
                      (let [updated-metadata (get-metadata uid-one pack-id)]
                        (is (= "Added"
                               (get-in updated-metadata [:body ::ms/description])))
                        (is (= #{"New Tag"}
                               (get-in updated-metadata [:body ::ms/tags])))
                        (is (= #{file-one-id file-two-id}
                               (get-in updated-metadata [:body ::ms/packed-ids]))))))))