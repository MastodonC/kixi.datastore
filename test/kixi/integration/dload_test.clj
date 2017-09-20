(ns kixi.integration.dload-test
  {:integration true}
  (:require [clojure.test :refer :all]
            [clj-http.client :as client]
            [kixi.datastore
             [filestore :as fs]
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
        resp (client/get link {:as :stream})
        ^String cd (get-in resp [:headers "Content-Disposition"])]
    (is (.endsWith
         cd
         ".csv"))
    (bs/transfer (:body resp)
                 f)
    f))

(deftest round-trip-small-file
  (let [uid (uuid)
        filename "./test-resources/metadata-one-valid.csv"
        md-resp (send-file-and-metadata
                 (create-metadata uid filename))]
    (when-success md-resp
      (let [link (get-dload-link uid (get-in md-resp [:body ::ms/id]))]
        (is (files-match? filename (dload-file-link link)))
        (let [http-resp (file-redirect-by-id uid (get-in md-resp [:body ::ms/id]))]
          (is (= 302 (:status http-resp)))
          (is (get-in http-resp [:headers "Location"]))
          (when-let [location (get-in http-resp [:headers "Location"])]
            (if (clojure.string/starts-with? location "file:")
              (is (files-match? filename (slurp location)))
              (let [redirected (client/get location)]
                (is (= (get-in redirected [:headers "Content-Disposition"])
                       (str "attachment; filename=metadata-one-valid.csv"))) ;; extra .csv is a side effect of `send-file-and-metadata`
                (is (= (slurp filename)
                       (:body redirected)))))))))))
