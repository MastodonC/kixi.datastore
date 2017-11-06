(ns kixi.datastore.filestore.command-handler
  (:require [clojure.spec.alpha :as spec]
            [medley.core :as m]
            [taoensso.timbre :as log]
            [kixi.datastore.schemastore.utils :as sh]
            [kixi.datastore.filestore :as fs :refer [FileStoreUploadCache]]))

(sh/alias 'event 'kixi.event)
(sh/alias 'ms 'kixi.datastore.metadatastore)
(sh/alias 'up 'kixi.datastore.filestore.upload)
(sh/alias 'up-reject 'kixi.event.file.upload.rejection)

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn calc-chunk-ranges
  [size-per-part file-size]
  (let [num-chunks (/ file-size size-per-part)
        num-major-chunks (Math/floor num-chunks)
        get-start (fn [x]
                    (+ (get x :start-byte 0) (get x :length-bytes 0)))
        chunk-ranges (reduce (fn [a x]
                               (conj a {:start-byte (get-start (last a))
                                        :length-bytes size-per-part})) [] (range num-major-chunks))]
    (conj chunk-ranges {:start-byte (get-start (last chunk-ranges))
                        :length-bytes (- file-size (* (count chunk-ranges) size-per-part))})))

(defn add-ns
  [ns m]
  (m/map-keys (comp (partial keyword (name ns)) name) m))

(defn create-upload-cmd-handler
  [link-fn]
  (fn [e]
    (let [id (uuid)]
      {:kixi.comms.event/key ::fs/upload-link-created
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/partition-key id
       :kixi.comms.event/payload {::fs/upload-link (link-fn id)
                                  ::fs/id id
                                  :kixi.user/id (get-in e [:kixi.comms.command/user :kixi.user/id])}})))

(defn reject-file-upload
  ([file-id reason]
   (reject-file-upload file-id reason nil))
  ([file-id reason message]
   [(merge {::event/type :kixi.datastore.filestore/file-upload-rejected
            ::event/version "1.0.0"
            ::up-reject/reason reason
            ::fs/id file-id}
           (when message
             {::up-reject/message message}))
    {:partition-key file-id}]))

(def small-file-size 10000000) ;; 10MB

(defn create-initiate-file-upload-cmd-handler
  [init-small-file-upload-creator-fn
   init-multi-part-file-upload-creator-fn
   cache]
  (fn [cmd]
    (let [size-bytes (::up/size-bytes cmd)
          id (uuid)
          part-ranges (calc-chunk-ranges small-file-size size-bytes)
          mup? (> (count part-ranges) 1)
          {:keys [upload-parts upload-id]}
          (if mup?
            (init-multi-part-file-upload-creator-fn id part-ranges)
            {:upload-id "local"
             :upload-parts (update part-ranges 0 assoc :url (init-small-file-upload-creator-fn id))})]
      (fs/put-item! cache id mup? (:kixi/user cmd) upload-id)
      [{::event/type ::fs/file-upload-initiated
        ::event/version "1.0.0"
        ::up/part-urls (m/map-vals (partial add-ns :kixi.datastore.filestore.upload) upload-parts)
        ::fs/id id}
       {:partition-key id}])))

(defn create-complete-file-upload-cmd-handler
  [complete-small-file-upload-creator-fn
   complete-multi-part-file-upload-creator-fn
   cache]
  (fn [cmd]
    (let [{:keys [kixi.datastore.filestore.upload/part-ids
                  kixi.datastore.filestore/id
                  kixi/user]} cmd]
      (let [upload (not-empty (fs/get-item cache id))]
        (cond
          (not (spec/valid? :kixi/command cmd)) (reject-file-upload id :invalid-cmd)
          (not upload) (reject-file-upload id :unauthorised)
          (not (= (:kixi.user/id user) (get-in upload [:kixi/user :kixi.user/id]))) (reject-file-upload id :unauthorised)
          :else (let [[ok? reason msg] (if (::up/mup? upload)
                                         (complete-multi-part-file-upload-creator-fn id part-ids upload)
                                         (complete-small-file-upload-creator-fn id part-ids upload))]
                  (if ok?
                    [{::event/type ::fs/file-upload-completed
                      ::event/version "1.0.0"
                      ::fs/id id}
                     {:partition-key id}]
                    (reject-file-upload id reason msg))))))))
