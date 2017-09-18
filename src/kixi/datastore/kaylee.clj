(ns kixi.datastore.kaylee
  (:require [kixi.datastore
             [metadatastore :as ms]
             [communication-specs :as cs]]
            [kixi.comms :as c]
            [com.gfredericks.schpec :as sh]))

(sh/alias 'ke 'kixi.comms.event)


;; This namespace is for useful functions, designed too make ops a bunch easier

(println "<<< THE CURRENT PROFILE IS:" @kixi.datastore.application/profile ">>>")

(defn system
  []
  @kixi.datastore.application/system)

(defn metadatastore
  []
  (:metadatastore (system)))

(defn comms
  []
  (:communications @kixi.datastore.application/system))


(defn get-metadata
  [meta-id]
  (ms/retrieve (metadatastore) meta-id))

(defn send-sharing-update
  "Issues an event with sharing matrix changes, bypasses the command level user authorization."
  [user-id user-group metadata-id change-type activity target-group]
  {:pre [string? string? string? keyword? keyword? string?]}
  (c/send-event!
   (comms)
   :kixi.datastore.file-metadata/updated
   "1.0.0"   
   {::cs/file-metadata-update-type ::cs/file-metadata-sharing-updated
    ::ms/id metadata-id
    ::ms/sharing-update change-type
    ::ms/activity activity
    :kixi.group/id target-group
    :kixi/user {:kixi.user/id user-id
                :kixi.user/groups [user-group]}}
   {::ke/partition-key metadata-id}))
