(ns kixi.integration.upload-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go]]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.comms :as c]
            [kixi.integration.base :refer :all]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.datastore.system :as system]
            [kixi.repl :as repl]
            [kixi.datastore.filestore :as fs]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as ms]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [kixi.repl :as repl]
            [clojure.java.io :as io]
            [kixi.datastore.schemastore :as ss])
  (:import [java.io
            File
            FileNotFoundException]))


(def uid (uuid))

(def comms (atom nil))

(defn extract-comms
  [all-tests]
  (reset! comms (:communications @repl/system))
  (all-tests)
  (reset! comms nil))

(defn attach-event-handler!
  [group-id event handler]
  (c/attach-event-handler!
   @comms
   group-id
   event
   "1.0.0"
   handler))

(defn attach-create-upload-link
  [receiver]
  (attach-event-handler!
   :get-upload-link
   :kixi.datastore.filestore/upload-link-created
   #(do (reset! receiver %)
        nil)))

(defn detach-handler
  [handler]
  (c/detach-handler!
   @comms
   handler))

(use-fixtures :once cycle-system-fixture extract-comms)

(defn send-upload-link-cmd
  []
  (c/send-command!
   @comms
   :kixi.datastore.filestore/create-upload-link
   "1.0.0" {}))

(defn send-metadata-cmd
  [metadata]
  (c/send-command!
   @comms
   :kixi.datastore.filestore/create-file-metadata
   "1.0.0" metadata))

(defn get-upload-link-event
  []
  (let [result (atom nil)
        handler (attach-create-upload-link
                  result)]    
    (send-upload-link-cmd)
    (wait-for-pred #(deref result))
    (detach-handler handler)
    @result))

(defn get-upload-link
  []
  (let [link-event (get-upload-link-event)]
    [(get-in link-event [:kixi.comms.event/payload :kixi.datastore.filestore/upload-link])
     (get-in link-event [:kixi.comms.event/payload :kixi.datastore.filestore/id])]))

(deftest create-link-test
  (let [link (get-upload-link-event)]
    (is link)
    (when-let [r link]
      (is (get-in r [:kixi.comms.event/payload :kixi.datastore.filestore/upload-link]))
      (is (get-in r [:kixi.comms.event/payload :kixi.datastore.filestore/id])))))

(defmulti upload-file  
  (fn [^String target file-name]
    (subs target 0
          (.indexOf target
                    ":"))))

(defn strip-protocol
  [^String path]
  (subs path
        (+ 3 (.indexOf path
                       ":"))))

(defmethod upload-file "file"
  [target file-name]
  (io/copy (io/file file-name)
           (doto (io/file (strip-protocol target))
             (.createNewFile))))

(defn deliver-file-and-metadata
  ([uid file-name]
   (deliver-file-and-metadata uid file-name nil))
  ([uid file-name schema-id]
   (deliver-file-and-metadata (merge {:file-name file-name
                                      ::ms/name file-name
                                      ::ms/type "stored"
                                      ::ms/sharing {::ms/file-read [uid]
                                                    ::ms/meta-read [uid]}
                                      ::ms/provenance {::ms/source "upload"
                                                       :kixi.user/id uid}
                                      ::ms/size-bytes (file-size file-name)
                                      ::ms/header true}
                                     (when schema-id
                                       {::ss/id schema-id}))))
  ([{:keys [file-name]
     :as metadata}]
   (let [[link id] (get-upload-link)]
     (is link)
     (is id)
     (when link
       (upload-file link
                    file-name)
       (send-metadata-cmd (-> metadata
                              (dissoc :file-name)
                              (assoc ::ms/id id)))
       (wait-for-metadata-key uid
                              id
                              ::ms/id)))))

(deftest round-trip-small-file
  (let [md-resp (deliver-file-and-metadata uid
                                           "./test-resources/metadata-one-valid.csv")]
    (when-success md-resp
      (when-let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-one-valid.csv"
             (dload-file uid locat)))))))

(deftest round-trip-files

  (let [r (post-file uid
                     "./test-resources/metadata-12MB-valid.csv")]
    (is (= 201
           (:status r))
        (str "Reason: " (parse-json (:body r))))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/metadata-12MB-valid.csv"
           (dload-file uid locat)))))
  (let [r (post-file uid
                     "./test-resources/metadata-344MB-valid.csv")]
    (is (= 201
           (:status r))
        (str "Reason: " (parse-json (:body r))))
    (when-let [locat (get-in r [:headers "Location"])]
      (is (files-match?
           "./test-resources/metadata-344MB-valid.csv"
           (dload-file uid locat))))))
