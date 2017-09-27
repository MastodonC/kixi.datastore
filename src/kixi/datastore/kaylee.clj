(ns kixi.datastore.kaylee
  (:require [kixi.datastore
             [metadatastore :as ms]
             [communication-specs :as cs]]
            [kixi.comms :as c]
            [kixi.datastore.schemastore.utils :as sh]))

(sh/alias 'ke 'kixi.comms.event)

(def kaylee-user-group "00000000-0000-0000-0000-000000000000")


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
  (:communications (system)))

(defn get-metadata
  [meta-id]
  {:pre [string?]}
  (ms/retrieve (metadatastore) meta-id))

(defn get-metadata-by-group
  [group-ids from-dex page-count]
  {:pre [#(and (vector? group-ids)
               (every? string? group-ids))
         integer? integer?]}
  (ms/query (metadatastore)
            {:kixi.user/groups group-ids}
            from-dex
            page-count
            [::ms/provenance ::ms/created]
            "desc"))

(defn send-sharing-update
  "Issues an event with sharing matrix changes, bypasses the command level user authorization."
  [your-user-id metadata-id change-type activity target-group]
  {:pre [string? string? keyword? keyword? string?]}
  (c/send-event!
   (comms)
   :kixi.datastore.file-metadata/updated
   "1.0.0"   
   {::cs/file-metadata-update-type ::cs/file-metadata-sharing-updated
    ::ms/id metadata-id
    ::ms/sharing-update change-type
    ::ms/activity activity
    :kixi.group/id target-group
    :kixi/user {:kixi.user/id your-user-id
                :kixi.user/groups [kaylee-user-group]}}
   {::ke/partition-key metadata-id}))

(defn remove-all-sharing
  "Issues events for all entries in the sharing matrix for the metadata-id."
  [your-user-id metadata-id]
  {:pre [string? string? string?]}
  (let [metadata (get-metadata metadata-id)]
    (doseq [activity (keys (::ms/sharing metadata))
            target-group (get-in metadata [::ms/sharing activity])]
      (send-sharing-update your-user-id metadata-id ::ms/sharing-disj activity target-group))))
