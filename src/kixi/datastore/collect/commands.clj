(ns kixi.datastore.collect.commands
  (:require [clojure.spec.alpha :as s]
            [kixi.comms :as comms]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'c  'kixi.datastore.collect)
(sh/alias 'ms 'kixi.datastore.metadatastore)

(defmethod comms/command-payload
  [:kixi.datastore.collect/request-collection "1.0.0"]
  [_]
  (s/keys :req [::c/message
                ::c/groups
                ::ms/id]))
