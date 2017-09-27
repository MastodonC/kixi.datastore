(ns kixi.datastore.communication-specs
  (:require [clojure.spec.alpha :as s]
            [kixi.comms :as c]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]
             [segmentation :as seg]
             [filestore :as fs]]))

(s/def ::event #{:kixi.datastore/file-created
                 :kixi.datastore/file-metadata-updated
                 :kixi.datastore/file-segmentation-created
                 :kixi.datastore.schema/created
                 :kixi.datastore.filestore/download-link-created
                 :kixi.datastore.filestore/download-link-rejected})

(s/def ::version (s/and string? #(re-matches #"\d+\.\d+\.\d+" %)))


(defmulti file-metadata-updated-type ::file-metadata-update-type)

(defmethod file-metadata-updated-type ::file-metadata-created
  [_]
  (s/keys :req [::file-metadata-update-type ::ms/file-metadata]))

(defmethod file-metadata-updated-type ::file-metadata-segmentation-add
  [_]
  (s/keys :req [::file-metadata-update-type ::ms/segmentation]))


(defmethod file-metadata-updated-type ::file-metadata-structural-validation-checked
  [_]
  (s/keys :req [::file-metadata-update-type ::ms/structural-validation ::ms/id]))

(s/def ::file-metadata-updated
  (s/multi-spec file-metadata-updated-type ::file-metadata-update-type))


(defmulti payloads ::event)

(defmethod payloads :kixi.datastore/file-created
  [_]
  ::ms/file-metadata)

(defmethod payloads :kixi.datastore/file-metadata-updated
  [_]
  ::file-metadata-updated)

(defmethod payloads :kixi.datastore/file-segmentation-created
  [_]
  (s/keys :req [::seg/id ::ms/id ::seg/column-name :kixi.user/id]))

(defmethod payloads :kixi.datastore.schema/created
  [_]
  ::ss/create-schema-request)

(defmethod payloads :kixi.datastore.filestore/download-link-created
  [_]
  (s/keys :req [::ms/id
                :kixi/user
                ::fs/link]))

(defmethod payloads :kixi.datastore.filestore/download-link-rejected
  [_]
  (s/keys :req [::ms/id
                :kixi/user]))

(s/def ::payloads
  (s/multi-spec payloads ::event))

(s/fdef send-event!
        :args (s/cat :comms (partial instance? kixi.comms.Communications)
                     :payload ::payloads))

(defn send-event!
  [comms payload-plus]
  (c/send-event! comms
                 (::event payload-plus) 
                 (::version payload-plus)
                 (dissoc payload-plus
                         ::event
                         ::version)))
