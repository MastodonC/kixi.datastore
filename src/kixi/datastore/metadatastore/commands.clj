(ns kixi.datastore.metadatastore.commands
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.utils :as sh]
            [kixi.comms :as comms]))

(sh/alias 'cmd 'kixi.command)
(sh/alias 'user 'kixi.user)
(sh/alias 'md 'kixi.datastore.metadatastore)

(defmethod comms/command-payload
  [:kixi.datastore/delete-file "1.0.0"]
  [_]
  (s/keys :req [::md/id]))

(defmethod comms/command-payload
  [:kixi.datastore/delete-bundle "1.0.0"]
  [_]
  (s/keys :req [::md/id]))

(defmethod comms/command-payload
  [:kixi.datastore/add-files-to-bundle "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/bundled-ids]))

(defmethod comms/command-payload
  [:kixi.datastore/remove-files-from-bundle "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/bundled-ids]))
