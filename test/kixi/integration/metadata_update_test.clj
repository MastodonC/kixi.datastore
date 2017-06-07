(ns kixi.integration.metadata-update-test
  (:require [clojure.test :refer :all]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]]
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
            event (update-metadata-sharing
                   uid "Invalid-Meta-ID"
                   ::ms/sharing-conj 
                   ::ms/file-read
                   new-group)]
        (when-event-key event :kixi.datastore.metadatastore/sharing-change-rejected
                        (is (= :invalid
                               (get-in event [:kixi.comms.event/payload :reason]))))))))

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

