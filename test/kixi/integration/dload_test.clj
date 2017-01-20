(ns kixi.integration.dload-test
  (:require [clojure.test :refer :all]
            [clj-http.client :as client]
            [kixi.datastore
             [filestore :as fs]
             [metadata-creator :as mdc]
             [metadatastore :as ms]]
            [kixi.integration.base :refer :all]
            [byte-streams :as bs])
  (:import [java.io File]))


(use-fixtures :once cycle-system-fixture extract-comms)

(defmulti dload-file-link (fn [^String link]
                            (.substring link 0 (.indexOf link ":"))))

(defmethod dload-file-link
  "file"
  [^String link]
  (File. (.substring link (inc (.indexOf link "://")))))

(defmethod dload-file-link
  "https"
  [^String link]
  (let [f (java.io.File/createTempFile (uuid) ".tmp")
        _ (.deleteOnExit f)
        resp (client/get link {:as :stream})]
    (is (.endsWith 
         (get-in resp [:headers "Content-Disposition"])
         ".csv"))
    (bs/transfer (:body resp)
                 f)
    f))

(deftest round-trip-small-file
  (let [uid (uuid)
        md-resp (send-file-and-metadata 
                 (create-metadata uid
                                  "./test-resources/metadata-one-valid.csv"))]
    (when-success md-resp
      (let [link (get-dload-link uid (get-in md-resp [:body ::ms/id]))]
        (is (files-match?
             "./test-resources/metadata-one-valid.csv"
             (dload-file-link link)))))))

