(ns kixi.datastore.collect.command-handler
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.collect.commands]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'event 'kixi.event)
(sh/alias 'c 'kixi.datastore.collect)
(sh/alias 'c-reject 'kixi.event.collect.rejection)

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn reject-collection-request
  ([reason]
   (reject-collection-request reason nil))
  ([reason message]
   [(merge {::event/type :kixi.datastore.collect/collection-request-rejected
            ::event/version "1.0.0"
            ::c-reject/reason reason}
           (when message
             {::c-reject/message message}))
    {:partition-key (uuid)}]))

(defn send-collection-request-event!
  [sender groups message id]
  [{::event/type :kixi.datastore.collect/collection-requested
    ::event/version "1.0.0"
    ::c/message message
    ::c/groups groups
    ::c/sender sender}
   {:partition-key id}])

(defn create-request-collection-handler
  [metadatastore]
  (fn [cmd]
    (let [{:keys [kixi/user
                  ::c/groups
                  ::c/message
                  ::ms/id]} cmd
          bundle (ms/retrieve metadatastore id)]
      (cond
        (not (s/valid? :kixi/command cmd))
        (reject-collection-request :invalid-cmd (with-out-str (s/explain :kixi/command cmd)))

        (not (ms/bundle? bundle))
        (reject-collection-request :incorrect-type)

        (not (ms/authorised-fn metadatastore ::ms/meta-update id (:kixi.user/groups user)))
        (reject-collection-request :unauthorised)

        :else (send-collection-request-event! user groups message id)))))
