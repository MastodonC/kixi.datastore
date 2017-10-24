(ns kixi.datastore.filestore.events
  (:require [clojure.spec.alpha :as s]
            [kixi.comms :as comms]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'ms 'kixi.datastore.metadatastore)
(sh/alias 'f 'kixi.datastore.filestore)
(sh/alias 'up 'kixi.datastore.filestore.upload)

(defmethod comms/event-payload
  [:kixi.datastore.filestore/multi-part-upload-links-created "1.0.0"]
  [_]
  (s/keys :req [::up/part-urls
                ::up/id
                ::f/id]))

(defmethod comms/event-payload
  [:kixi.datastore.filestore/multi-part-upload-completed "1.0.0"]
  [_]
  (s/keys :req [::up/id
                ::f/id]))
