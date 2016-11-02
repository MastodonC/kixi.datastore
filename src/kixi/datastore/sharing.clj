(ns kixi.datastore.sharing
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore :as ms]))

(defmulti authorisation
  (fn [file-metadatestore domain action id groups]
    [domain action]))

(defmethod authorisation
  [::ms/file-sharing :read]  
  [file-metadatastore domain action id groups]
  (ms/authorisation file-metadatastore
                    domain
                    action
                    id
                    groups))

(defmethod authorisation
  [::ms/file-metadata-sharing :read]  
  [file-metadatastore domain action id groups]
  (ms/authorisation file-metadatastore
                    domain
                    action
                    id
                    groups))

(defprotocol ISharing
  (authorised
    [this domain action id groups]))

(defrecord Sharing
    [metadatastore]
  ISharing
  (authorised
    [this domain action id groups]
    (authorisation metadatastore
                   domain
                   action
                   id
                   groups))  
  component/Lifecycle
  (start [component]
    component)
  (stop [component]
    component))
