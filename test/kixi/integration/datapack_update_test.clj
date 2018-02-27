(ns kixi.integration.datapack-update-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [kixi.comms :as c]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]]
            [kixi.integration.base :as base :refer :all]
            [medley.core :refer [dissoc-in]]))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest datapack-create-empty
  (let [uid (uuid)
        datapack-resp (send-datapack uid "Empty Datapack" #{})]
    (when-success datapack-resp
      (is (= #{}
             (get-in datapack-resp [:body ::ms/bundled-ids]))))))

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


(deftest bundle-delete
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [meta-id (get-in datapack-resp [:body ::ms/id])
            response-event (send-bundle-delete uid meta-id)]
        (when-event-type response-event :kixi.datastore/bundle-deleted
                         (wait-for-pred #(= 401
                                            (:status (get-metadata uid meta-id))))
                         (is (= 401
                                (:status (get-metadata uid meta-id)))))))))

(deftest ^:acceptance deleted-bundles-are-not-returned-in-searches
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [meta-id (get-in datapack-resp [:body ::ms/id])
            all-visible-cnt (get-in (search-metadata uid [::ms/meta-read])
                                    [:body :paging :count])]
        (is (= 2
               all-visible-cnt))
        (let [response-event (send-bundle-delete uid meta-id)]
          (when-event-type response-event :kixi.datastore/bundle-deleted
                           (wait-for-pred #(= 401
                                              (:status (get-metadata uid meta-id))))
                           (let [only-file-visible-cnt (get-in (search-metadata uid [::ms/meta-read])
                                                               [:body :paging :count])]
                             (is (= 1
                                    only-file-visible-cnt)))))))))

(deftest bundle-delete-invalid-cmd-rejected
  (let [response-event (binding [c/*validate-commands* false]
                         (send-bundle-delete (uuid) "foobar"))]
    (when-event-type response-event :kixi.datastore/bundle-delete-rejected
                     (is (= :invalid-cmd
                            (:reason response-event))))))

(deftest bundle-delete-unauthorised-rejected
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [meta-id (get-in datapack-resp [:body ::ms/id])
            response-event (send-bundle-delete (uuid) meta-id)]
        (when-event-type response-event :kixi.datastore/bundle-delete-rejected
                         (is (= :unauthorised
                                (:reason response-event)))
                         (is (= 200
                                (:status (get-metadata uid meta-id)))))))))

(deftest bundle-delete-incorrect-type-rejected
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [file-meta-id (first (get-in datapack-resp [:body ::ms/bundled-ids]))
            response-event (send-bundle-delete uid file-meta-id)]
        (when-event-type response-event :kixi.datastore/bundle-delete-rejected
                         (is (= :incorrect-type
                                (:reason response-event)))
                         (is (= 200
                                (:status (get-metadata uid file-meta-id)))))))))

(deftest add-files-to-bundle-invalid-cmd-rejected
  (let [response-event (binding [c/*validate-commands* false]
                         (send-add-files-to-bundle (uuid) "12345" #{"foobar"}))] ;; invalid id
    (when-event-type response-event :kixi.datastore/files-add-to-bundle-rejected
                     (is (= :invalid-cmd
                            (:reason response-event))))))

(deftest ^:acceptance add-files-to-bundle-incorrect-type-rejected
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [file-meta-id (first (get-in datapack-resp [:body ::ms/bundled-ids]))
            response-event (send-add-files-to-bundle uid file-meta-id #{file-meta-id})]
        (when-event-type response-event :kixi.datastore/files-add-to-bundle-rejected
                         (is (= :incorrect-type
                                (:reason response-event))))))))

(deftest add-files-to-bundle-unauthorised-rejected
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [bundle-id (get-in datapack-resp [:body ::ms/id])
            file-meta-id (first (get-in datapack-resp [:body ::ms/bundled-ids]))
            response-event (send-add-files-to-bundle uid (uuid) bundle-id #{file-meta-id})]
        (when-event-type response-event :kixi.datastore/files-add-to-bundle-rejected
                         (is (= :unauthorised
                                (:reason response-event))))))))

(deftest add-files-to-bundle
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [bundle-id (get-in datapack-resp [:body ::ms/id])
            first-file-id (first (get-in datapack-resp [:body ::ms/bundled-ids]))
            metadata-response (send-file-and-metadata
                               (create-metadata
                                uid
                                "./test-resources/metadata-one-valid.csv"))]
        (when-success metadata-response
          (let [extra-file-id (get-in metadata-response [:body ::ms/id])
                response-event (send-add-files-to-bundle uid bundle-id #{extra-file-id})]
            (when-event-type response-event :kixi.datastore/files-added-to-bundle
                             (wait-for-pred #(= 2
                                                (count (get-in (get-metadata uid bundle-id) [:body ::ms/bundled-ids]))))
                             (let [bundled-ids (get-in (get-metadata uid bundle-id) [:body ::ms/bundled-ids])]
                               (is (= #{first-file-id extra-file-id}
                                      bundled-ids))))))))))

(deftest add-files-to-bundle-bundle-add-only
  (let [owner (uuid)
        contributor (uuid)
        datapack-resp (send-datapack owner "Empty Datapack" #{})]
    (when-success datapack-resp
      (let [bundle-id (get-in datapack-resp [:body ::ms/id])
            metadata-response (send-file-and-metadata
                               contributor contributor
                               (create-metadata
                                contributor
                                "./test-resources/metadata-one-valid.csv"))]
        (when-success metadata-response
          (let [file-id (get-in metadata-response [:body ::ms/id])
                response-event (send-add-files-to-bundle contributor bundle-id #{file-id})]
            (when-event-type response-event :kixi.datastore/files-add-to-bundle-rejected
                             (update-metadata-sharing owner bundle-id ::ms/sharing-conj ::ms/bundle-add contributor)
                             (let [response-event (send-add-files-to-bundle contributor bundle-id #{file-id})]
                               (when-event-type response-event :kixi.datastore/files-added-to-bundle
                                                (wait-for-pred #(= 1
                                                                   (count (get-in (get-metadata owner bundle-id) [:body ::ms/bundled-ids]))))
                                                (let [bundled-ids (get-in (get-metadata owner bundle-id) [:body ::ms/bundled-ids])]
                                                  (is (= #{file-id}
                                                         bundled-ids))))))))))))

(deftest ^:acceptance remove-files-from-bundle-incorrect-type-rejected
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [file-meta-id (first (get-in datapack-resp [:body ::ms/bundled-ids]))
            response-event (send-remove-files-from-bundle uid file-meta-id #{file-meta-id})]
        (when-event-type response-event :kixi.datastore/files-remove-from-bundle-rejected
                         (is (= :incorrect-type
                                (:reason response-event))))))))

(deftest ^:acceptance remove-files-to-bundle-unauthorised-rejected
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when-success datapack-resp
      (let [bundle-id (get-in datapack-resp [:body ::ms/id])
            file-meta-id (first (get-in datapack-resp [:body ::ms/bundled-ids]))
            response-event (send-remove-files-from-bundle uid (uuid) bundle-id #{file-meta-id})]
        (when-event-type response-event :kixi.datastore/files-remove-from-bundle-rejected
                         (is (= :unauthorised
                                (:reason response-event))))))))

(deftest remove-files-from-bundle
  (let [uid (uuid)
        file-one (send-file-and-metadata
                  (create-metadata
                   uid
                   "./test-resources/metadata-one-valid.csv"))
        file-one-id (get-in file-one [:body ::ms/id])
        file-two (send-file-and-metadata
                  (create-metadata
                   uid
                   "./test-resources/metadata-one-valid.csv"))
        file-two-id (get-in file-two [:body ::ms/id])
        datapack-resp (send-datapack uid "test remove-files-from-bundle two files" #{file-one-id file-two-id})]
    (when-success datapack-resp
      (let [bundle-id (get-in datapack-resp [:body ::ms/id])]
        (is (= #{file-one-id file-two-id}
               (set (get-in datapack-resp [:body ::ms/bundled-ids]))))
        (let [response-event (send-remove-files-from-bundle uid bundle-id #{file-two-id})]
          (when-event-type response-event :kixi.datastore/files-removed-from-bundle
                           (wait-for-pred #(= 1
                                              (count (get-in (get-metadata uid bundle-id) [:body ::ms/bundled-ids]))))
                           (let [bundled-ids (get-in (get-metadata uid bundle-id) [:body ::ms/bundled-ids])]
                             (is (= #{file-one-id}
                                    bundled-ids)))
                           (let [response-event (send-remove-files-from-bundle uid bundle-id #{file-one-id})]
                             (when-event-type response-event :kixi.datastore/files-removed-from-bundle
                                              (wait-for-pred #(= 0
                                                                 (count (get-in (get-metadata uid bundle-id) [:body ::ms/bundled-ids]))))
                                              (let [bundled-ids (get-in (get-metadata uid bundle-id) [:body ::ms/bundled-ids])]
                                                (is (= #{}
                                                       bundled-ids)))))))))))

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
                 {:kixi.datastore.metadatastore.update/bundled-ids {:conj #{file-two-id}}
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
                               (get-in updated-metadata [:body ::ms/bundled-ids]))))))))
