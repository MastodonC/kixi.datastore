(ns kixi.datastore.filestore.events
  (:require [clojure.spec.alpha :as s]
            [kixi.comms :as comms]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'fs  'kixi.datastore.filestore)
(sh/alias 'up  'kixi.datastore.filestore.upload)
(sh/alias 'up-fail 'kixi.event.file.upload.failure)
(sh/alias 'up-reject 'kixi.event.file.upload.rejection)

(s/def ::up-fail/reason
  #{:invalid-cmd})

(s/def ::up-reject/reason
  #{:invalid-cmd
    :file-missing
    :unauthorised
    :data-too-small})

(s/def ::up-fail/message string?)
(s/def ::up-reject/message string?)

(defmethod comms/event-payload
  [:kixi.datastore.filestore/file-upload-initiated "1.0.0"]
  [_]
  (s/keys :req [::up/part-urls
                ::fs/id]))

(defmethod comms/event-payload
  [:kixi.datastore.filestore/file-upload-completed "1.0.0"]
  [_]
  (s/keys :req [::fs/id]))

(defmethod comms/event-payload
  [:kixi.datastore.filestore/file-upload-rejected "1.0.0"]
  [_]
  (s/keys :req [::fs/id
                ::up-reject/reason]
          :opt [::up-reject/message]))

(defmethod comms/event-payload
  [:kixi.datastore.filestore/file-upload-failed "1.0.0"]
  [_]
  (s/keys :req [::up-fail/reason]
          :opt [::up-fail/message]))
