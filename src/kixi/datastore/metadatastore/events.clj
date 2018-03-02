(ns kixi.datastore.metadatastore.events
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore.utils :as sh]
            [kixi.comms :as comms]))

(sh/alias 'event 'kixi.event)
(sh/alias 'cmd 'kixi.command)
(sh/alias 'user 'kixi.user)
(sh/alias 'md 'kixi.datastore.metadatastore)
(sh/alias 'mdr 'kixi.datastore.metadatastore.relaxed)

(s/def ::spec-explain any?)

;; Files

(defmethod comms/event-payload
  [:kixi.datastore/file-deleted "1.0.0"]
  [_]
  (s/keys :req [::md/id]))

(sh/alias 'fd-reject 'kixi.event.file.delete.rejection)

(s/def ::fd-reject/reason
  #{:unauthorised
    :incorrect-metadata-type
    :invalid-cmd})

(defmethod comms/event-payload
  [:kixi.datastore/file-delete-rejected "1.0.0"]
  [_]
  (s/and (s/keys :req-un [::fd-reject/reason]
                 :opt [::md/id]
                 :opt-un [::spec-explain])
         (fn invalid-cmd-is-explained?
           [m]
           (if (= (:reason m) :invalid-cmd)
             (contains? m :spec-explain)
             true))))

;; Bundles

(defmethod comms/event-payload
  [:kixi.datastore/bundle-deleted "1.0.0"]
  [_]
  (s/keys :req [::md/id]))

(sh/alias 'dd-reject 'kixi.event.bundle.delete.rejection)

(s/def ::dd-reject/reason
  #{:unauthorised
    :incorrect-type
    :invalid-cmd})

(defmethod comms/event-payload
  [:kixi.datastore/bundle-delete-rejected "1.0.0"]
  [_]
  (s/keys :req [::md/id]
          :req-un [::dd-reject/reason]
          :opt-un [::spec-explain]))

(defmethod comms/event-payload
  [:kixi.datastore/bundle-delete-rejected "2.0.0"]
  [_]
  (s/keys :req [::mdr/id]
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
  [:kixi.datastore/files-add-to-bundle-rejected "2.0.0"]
  [_]
  (s/keys :req [::mdr/id
                ::mdr/bundled-ids]
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

(sh/alias 'sharing-reject 'kixi.event.metadata.sharing-change.rejection)

(s/def ::sharing-reject/reason
  #{:unauthorised
    :invalid-cmd})

(s/def ::sharing-reject/explain string?)
(s/def ::sharing-reject/original any?)

(defmethod comms/event-payload
  [:kixi.datastore/sharing-change-rejected "2.0.0"]
  [_]
  (s/keys :req [::sharing-reject/reason]
          :opt [::sharing-reject/explain
                ::sharing-reject/original
                ::md/id]))

;;

(defmethod comms/event-payload
  [:kixi.datastore/sharing-changed "1.0.0"]
  [_]
  ::md/sharing-change-payload)
