(ns kixi.datastore.metadatastore.events
  (:require [clojure.spec :as s]
            [com.gfredericks.schpec :as sh]
            [kixi.comms :as comms]))

(sh/alias 'event 'kixi.event)
(sh/alias 'cmd 'kixi.command)
(sh/alias 'user 'kixi.user)
(sh/alias 'md 'kixi.datastore.metadatastore)

(defmethod comms/event-payload
  [:kixi.datastore/bundle-deleted "1.0.0"]
  [_]
  (s/keys :req [::md/id]))

(sh/alias 'dd-reject 'kixi.event.bundle.delete.rejection)

(s/def ::dd-reject/reason
  #{:unauthorised
    :incorrect-type
    :invalid-cmd})

(s/def ::spec-explain any?)

(defmethod comms/event-payload
  [:kixi.datastore/bundle-delete-rejected "1.0.0"]
  [_]
  (s/keys :req [::md/id]
          :req-un [::dd-reject/reason]
          :opt-un [::spec-explain]))
