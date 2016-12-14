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

(use-fixtures :once cycle-system-fixture extract-comms)

(deftest create-link-test
  (let [link (get-upload-link-event)]
    (is link)
    (when-let [r link]
      (is (get-in r [:kixi.comms.event/payload :kixi.datastore.filestore/upload-link]))
      (is (get-in r [:kixi.comms.event/payload :kixi.datastore.filestore/id])))))

(deftest round-trip-small-file
  (let [md-resp (deliver-file-and-metadata 
                 (create-metadata uid
                                  "./test-resources/metadata-one-valid.csv"))]
    (when-success md-resp
      (when-let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-one-valid.csv"
             (dload-file uid locat)))))))

(deftest round-trip-12M-file
  (let [md-resp (deliver-file-and-metadata 
                 (create-metadata uid
                                  "./test-resources/metadata-12MB-valid.csv"))]
    (when-success md-resp
      (when-let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-12MB-valid.csv"
             (dload-file uid locat)))))))

(deftest round-trip-344M-file  
  (let [md-resp (deliver-file-and-metadata 
                 (create-metadata uid
                                  "./test-resources/metadata-344MB-valid.csv"))]
    (when-success md-resp
      (when-let [locat (file-url (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-344MB-valid.csv"
             (dload-file uid locat)))))))

(deftest rejected-when-file-not-uploaded
  (is-file-metadata-rejected 
   #(send-metadata-cmd 
     (assoc (create-metadata uid
                             "./rejected-when-file-not-uploaded.non-file")
            ::ms/id (uuid)))
   {::fs/rejection-reason :file-not-exist}))

(deftest rejected-when-size-incorrect
  (is-file-metadata-rejected 
   #(deliver-file-and-metadata-no-wait
     (assoc (create-metadata uid
                             "./test-resources/metadata-one-valid.csv")
            ::ms/size-bytes 1))
   {::fs/rejection-reason :file-size-incorrect}))
