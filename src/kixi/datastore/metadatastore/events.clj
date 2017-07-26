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



(defmethod comms/event-payload
  [:kixi.datastore/files-added-to-bundle "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/bundled-ids]))

(sh/alias 'fab-reject 'kixi.event.bundle.addfiles.rejection)

(s/def ::fab-reject/reason
  #{:unauthorised
    :incorrect-type
    :invalid-cmd})

(defmethod comms/event-payload
  [:kixi.datastore/files-add-to-bundle-rejected "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/bundled-ids]
          :req-un [::fab-reject/reason]
          :opt-un [::spec-explain]))


(defmethod comms/event-payload
  [:kixi.datastore/files-removed-from-bundle "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/bundled-ids]))

(sh/alias 'frb-reject 'kixi.event.bundle.removefiles.rejection)

(s/def ::frb-reject/reason
  #{:unauthorised
    :incorrect-type
    :invalid-cmd})

(defmethod comms/event-payload
  [:kixi.datastore/files-remove-from-bundle-rejected "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/bundled-ids]
          :req-un [::fab-reject/reason]
          :opt-un [::spec-explain]))
