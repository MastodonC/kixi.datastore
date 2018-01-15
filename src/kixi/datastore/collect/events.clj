(ns kixi.datastore.collect.events
  (:require [clojure.spec.alpha :as s]
            [kixi.comms :as comms]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'c 'kixi.datastore.collect)
(sh/alias 'c-reject 'kixi.event.collect.rejection)

(defmethod comms/event-payload
  [:kixi.datastore.collect/collection-requested "1.0.0"]
  [_]
  (s/keys :req [::c/message
                ::c/groups
                ::c/sender]))

(s/def ::c-reject/reason
  #{:invalid-cmd
    :unauthorised})

(s/def ::c-reject/message string?)

(defmethod comms/event-payload
  [:kixi.datastore.collect/collection-request-rejected "1.0.0"]
  [_]
  (s/keys :req [::c-reject/reason]
          :opt [::c-reject/message]))
