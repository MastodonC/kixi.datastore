(ns kixi.datastore.metadatastore.commands
  (:require [clojure.spec :as s]
            [com.gfredericks.schpec :as sh]
            [kixi.comms :as comms]))

(sh/alias 'cmd 'kixi.command)
(sh/alias 'user 'kixi.user)
(sh/alias 'md 'kixi.datastore.metadatastore)

(defmethod comms/command-payload
  [:kixi.datastore/delete-bundle "1.0.0"]
  [_]
  (s/keys :req [::md/id]))



