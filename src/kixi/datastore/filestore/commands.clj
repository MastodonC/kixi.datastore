(ns kixi.datastore.filestore.commands
  (:require [clojure.spec.alpha :as s]
            [kixi.comms :as comms]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'f 'kixi.datastore.filestore)
(sh/alias 'up 'kixi.datastore.filestore.upload)

(defmethod comms/command-payload
  [:kixi.datastore.filestore/create-multi-part-upload-link "1.0.0"]
  [_]
  (s/keys :req [::up/part-count]))

(defmethod comms/command-payload
  [:kixi.datastore.filestore/complete-multi-part-upload "1.0.0"]
  [_]
  (s/keys :req [::up/part-ids
                ::up/id
                ::f/id]))
