(ns kixi.integration.metadata-update-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [kixi.comms :as c]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]]
            [kixi.datastore.metadatastore.license :as msl]
            [kixi.integration.base :as base :refer :all]
            [medley.core :refer [dissoc-in]]))

(defn metadata-file-schema
  [uid]
  {::ss/name ::metadata-file-schema
   ::ss/schema {::ss/type "list"
                ::ss/definition [:cola {::ss/type "integer"}
                                 :colb {::ss/type "integer"}]}
   ::ss/provenance {::ss/source "upload"
                    :kixi.user/id uid}
   ::ss/sharing {::ss/read [uid]
                 ::ss/use [uid]}})

(def get-schema-id (comp schema->schema-id metadata-file-schema))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest small-file-add-group-to-meta-read-command-validated-old
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (dissoc-in
                            (create-metadata
                             uid
                             "./test-resources/metadata-one-valid.csv")
                            [::ms/sharing ::ms/file-read]))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata-sharing-old
                   uid "Invalid-Meta-ID"
                   ::ms/sharing-conj
                   ::ms/file-read
                   new-group)]
        (when-event-key event :kixi.datastore.metadatastore/sharing-change-rejected
                        (is (= :invalid
                               (get-in event [:kixi.comms.event/payload :reason]))))))))

(deftest small-file-add-group-to-meta-read-command-validated
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (dissoc-in
                            (create-metadata
                             uid
                             "./test-resources/metadata-one-valid.csv")
                            [::ms/sharing ::ms/file-read]))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (binding [c/*validate-commands* false]
                    (update-metadata-sharing
                     uid "Invalid-Meta-ID"
                     ::ms/sharing-conj
                     ::ms/file-read
                     new-group))]
        (when-event-key event :kixi.datastore/sharing-change-rejected
                        (is (= :invalid-cmd
                               (:kixi.event.metadata.sharing-change.rejection/reason event))))))))

(deftest small-file-add-group-to-meta-read-old
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata-sharing-old
                   uid meta-id
                   ::ms/sharing-conj
                   ::ms/meta-read
                   new-group)]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (= #{uid new-group}
                                             (set (get-in metadata [:body ::ms/sharing ::ms/meta-read])))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{uid new-group}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-update]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/file-read]))))))))))

(deftest small-file-add-group-to-meta-read
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata-sharing
                   uid meta-id
                   ::ms/sharing-conj
                   ::ms/meta-read
                   new-group)]
        (when-event-key event :kixi.datastore/sharing-changed
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (= #{uid new-group}
                                             (set (get-in metadata [:body ::ms/sharing ::ms/meta-read])))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{uid new-group}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-update]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/file-read]))))))))))

(deftest small-file-add-group-to-new-file-read-old
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (dissoc-in
                            (create-metadata
                             uid
                             "./test-resources/metadata-one-valid.csv")
                            [::ms/sharing ::ms/file-read]))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata-sharing-old
                   uid meta-id
                   ::ms/sharing-conj
                   ::ms/file-read
                   new-group)]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (= #{new-group}
                                             (set (get-in metadata [:body ::ms/sharing ::ms/file-read])))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-update]))))
                          (is (= #{new-group}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/file-read]))))))))))

(deftest small-file-add-group-to-new-file-read
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (dissoc-in
                            (create-metadata
                             uid
                             "./test-resources/metadata-one-valid.csv")
                            [::ms/sharing ::ms/file-read]))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata-sharing
                   uid meta-id
                   ::ms/sharing-conj
                   ::ms/file-read
                   new-group)]
        (when-event-key event :kixi.datastore/sharing-changed
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (= #{new-group}
                                             (set (get-in metadata [:body ::ms/sharing ::ms/file-read])))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-update]))))
                          (is (= #{new-group}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/file-read]))))))))))

(deftest small-file-remove-group-from-file-read-old
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata-sharing-old
                   uid meta-id
                   ::ms/sharing-disj
                   ::ms/file-read
                   uid)]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (nil?
                                           (get-in metadata [:body ::ms/sharing ::ms/file-read]))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-update]))))
                          (is (nil?
                               (get-in updated-metadata [:body ::ms/sharing ::ms/file-read])))))))))

(deftest small-file-remove-group-from-file-read
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata-sharing
                   uid meta-id
                   ::ms/sharing-disj
                   ::ms/file-read
                   uid)]
        (when-event-key event :kixi.datastore/sharing-changed
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (nil?
                                           (get-in metadata [:body ::ms/sharing ::ms/file-read]))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-read]))))
                          (is (= #{uid}
                                 (set (get-in updated-metadata [:body ::ms/sharing ::ms/meta-update]))))
                          (is (nil?
                               (get-in updated-metadata [:body ::ms/sharing ::ms/file-read])))))))))

(deftest small-file-add-metadata
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/source-created {:set "20170615"}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (get-in metadata [:body ::ms/source-created])))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= "20170615"
                                 (get-in updated-metadata [:body ::ms/source-created])))))))))

(deftest small-file-remove-metadata
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/source-created {:set "20170615"}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (get-in metadata [:body ::ms/source-created])))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= "20170615"
                                 (get-in updated-metadata [:body ::ms/source-created]))))
                        (let [rm-event (update-metadata
                                        uid meta-id
                                        {:kixi.datastore.metadatastore.update/source-created :rm})]
                          (when-event-key event :kixi.datastore.file-metadata/updated
                                          (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                                            (not (get-in metadata [:body ::ms/source-created]))))
                                          (let [updated-metadata (get-metadata uid meta-id)]
                                            (is (nil?
                                                   (get-in updated-metadata [:body ::ms/source-created])))))))))))

(deftest ^:acceptance remove-on-nonexistant-field-allowed
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            new-group (uuid)
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/source-created :rm})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (not (get-in metadata [:body ::ms/source-created]))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (nil?
                               (get-in updated-metadata [:body ::ms/source-created])))))))))

(deftest ^:acceptance small-file-add-invalid-metadata
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            event (update-metadata
                   uid meta-id
                   {::ms/file-type "Invalid has no command"})]
        (when-event-key event :kixi.datastore.metadatastore/update-rejected
                        (is (= :invalid
                               (get-in event [:kixi.comms.event/payload :reason]))))))))

(deftest small-file-update-tags
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (assoc (create-metadata
                                   uid
                                   "./test-resources/metadata-one-valid.csv")
                                  ::ms/tags #{"orig1" "orig2"}))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/tags {:conj #{"Tag1" "Tag2" "Tag3"}}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (= #{"orig1" "orig2" "Tag1" "Tag2" "Tag3"}
                                             (get-in metadata [:body ::ms/tags]))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{"orig1" "orig2" "Tag1" "Tag2" "Tag3"}
                                 (get-in updated-metadata [:body ::ms/tags]))))
                        (let [disj-event (update-metadata
                                          uid meta-id
                                          {:kixi.datastore.metadatastore.update/tags {:disj #{"Tag1" "orig1"}}})]
                          (when-event-key disj-event :kixi.datastore.file-metadata/updated
                                          (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                                            (= #{"orig2" "Tag2" "Tag3"}
                                                               (get-in metadata [:body ::ms/tags]))))
                                          (let [updated-metadata (get-metadata uid meta-id)]
                                            (is (= #{"orig2" "Tag2" "Tag3"}
                                                   (get-in updated-metadata [:body ::ms/tags])))))))))))

(deftest small-file-update-tags-list
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (assoc (create-metadata
                                   uid
                                   "./test-resources/metadata-one-valid.csv")
                                  ::ms/tags #{"orig1" "orig2"}))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.update/tags {:conj ["Tag1" "Tag2" "Tag3"]}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (= #{"orig1" "orig2" "Tag1" "Tag2" "Tag3"}
                                             (get-in metadata [:body ::ms/tags]))))
                        (let [updated-metadata (get-metadata uid meta-id)]
                          (is (= #{"orig1" "orig2" "Tag1" "Tag2" "Tag3"}
                                 (get-in updated-metadata [:body ::ms/tags]))))
                        (let [disj-event (update-metadata
                                          uid meta-id
                                          {:kixi.datastore.metadatastore.update/tags {:disj ["Tag1" "orig1"]}})]
                          (when-event-key disj-event :kixi.datastore.file-metadata/updated
                                          (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                                            (= #{"orig2" "Tag2" "Tag3"}
                                                               (get-in metadata [:body ::ms/tags]))))
                                          (let [updated-metadata (get-metadata uid meta-id)]
                                            (is (= #{"orig2" "Tag2" "Tag3"}
                                                   (get-in updated-metadata [:body ::ms/tags])))))))))))

(deftest ^:acceptance small-file-update-license-type
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (assoc (create-metadata
                                   uid
                                   "./test-resources/metadata-one-valid.csv")
                                  ::ms/license {:kixi.datastore.metadatastore.license/type "old type"}))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.license.update/license {:kixi.datastore.metadatastore.license.update/type {:set "new type"}}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (= "new type"
                                             (get-in metadata [:body ::msl/license ::msl/type]))))
                        (let [metadata (get-metadata uid meta-id)]
                          (prn metadata)
                          (is (= "new type"
                                 (get-in metadata [:body ::msl/license ::msl/type])))))))))

(deftest ^:acceptance small-file-remove-license-type
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (assoc (create-metadata
                                   uid
                                   "./test-resources/metadata-one-valid.csv")
                                  ::ms/license {:kixi.datastore.metadatastore.license/type "old type"}))]
    (when-success metadata-response
      (let [meta-id (get-in metadata-response [:body ::ms/id])
            event (update-metadata
                   uid meta-id
                   {:kixi.datastore.metadatastore.license.update/license {:kixi.datastore.metadatastore.license.update/type :rm}})]
        (when-event-key event :kixi.datastore.file-metadata/updated
                        (wait-for-pred #(let [metadata (get-metadata uid meta-id)]
                                          (nil? (get-in metadata [:body ::msl/license ::msl/type]))))
                        (let [metadata (get-metadata uid meta-id)]
                          (is (nil? (get-in metadata [:body ::msl/license ::msl/type])))))))))

(deftest file-delete
  (let [uid (uuid)
        metadata-resp (send-file-and-metadata (create-metadata uid "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-resp
      (let [meta-id (get-in metadata-resp [:body ::ms/id])
            response-event (send-file-delete uid meta-id)]
        (when-event-type response-event :kixi.datastore/file-deleted
                         (wait-for-pred #(= 401
                                            (:status (get-metadata uid meta-id))))
                         (is (= 401
                                (:status (get-metadata uid meta-id)))))))))

(deftest ^:acceptance deleted-files-are-not-returned-in-searches
  (let [uid (uuid)
        metadata-resp (send-file-and-metadata (create-metadata uid "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-resp
      (let [meta-id (get-in metadata-resp [:body ::ms/id])
            all-visible-cnt (get-in (search-metadata uid [::ms/meta-read])
                                    [:body :paging :count])]
        (is (= 1
               all-visible-cnt))
        (let [response-event (send-file-delete uid meta-id)]
          (when-event-type response-event :kixi.datastore/file-deleted
                           (wait-for-pred #(= 401
                                              (:status (get-metadata uid meta-id))))
                           (let [only-file-visible-cnt (get-in (search-metadata uid [::ms/meta-read])
                                                               [:body :paging :count])]
                             (is (= 0
                                    only-file-visible-cnt)))))))))

(deftest ^:acceptance file-delete-unauthorised-rejected
  (let [uid (uuid)
        metadata-resp (send-file-and-metadata (create-metadata uid "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-resp
      (let [meta-id (get-in metadata-resp [:body ::ms/id])
            response-event (send-file-delete (uuid) meta-id)]
        (when-event-type response-event :kixi.datastore/file-delete-rejected
                         (is (= :unauthorised
                                (:reason response-event)))
                         (is (= 200
                                (:status (get-metadata uid meta-id)))))))))

(deftest ^:acceptance file-delete-incorrect-type-rejected
  (let [uid (uuid)
        datapack-resp (small-file-into-datapack uid)]
    (when datapack-resp
      (let [metadata-resp (send-file-and-metadata (create-metadata uid "./test-resources/metadata-one-valid.csv"))]
        (when-success metadata-resp
          (let [file-meta-id (get-in datapack-resp [:body ::ms/id])
                response-event (send-file-delete uid file-meta-id)]
            (when-event-type response-event :kixi.datastore/file-delete-rejected
                             (is (= :incorrect-metadata-type
                                    (:reason response-event)))
                             (is (= 200
                                    (:status (get-metadata uid file-meta-id)))))))))))
