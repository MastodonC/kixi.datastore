(ns kixi.integration.upload-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all :exclude [deftest]]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [digest :as d])
  (:import [java.io 
            File
            FileNotFoundException]))

(defmacro deftest
  [& everything]
  `(clojure.test/deftest ^:integration ~@everything))

(defn service-url
  []
  (or (System/getenv "SERVICE_URL") "localhost:8080"))

(def file-url (str "http://" (service-url) "/file"))

(defn metadata-url
  [id]
  (str file-url "/" id "/meta"))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn check-file
  [file-name]
  (let [f (io/file file-name)]
    (when-not (.exists f)
      (throw (new FileNotFoundException 
                  (str "File [" file-name "] not found. Have you run ./test-resources/create-test-files.sh?"))))))

(defn post-file
  [file-name]
  (check-file file-name)
  (client/post file-url
               {:multipart [{:name "file" :content (io/file file-name)}
                            {:name "name" :content "foo"}]}))

(defn dload-file
  [location]
  (let [f (java.io.File/createTempFile (uuid) ".tmp")
        _ (.deleteOnExit f)]
    (bs/transfer (:body (client/get location {:as :stream}))
                 f)
    f))

(defn get-metadata
  [id]
  (client/get (metadata-url id) {:as :json
                                 :accept :json}))

(defn extract-id
  [file-response]
  (let [locat (get-in file-response [:headers "Location"])]
    (subs locat (inc (clojure.string/last-index-of locat "/")))))

(defn files-match?
  [^String one ^File two]
  (= (d/md5 (File. one))
     (d/md5 two)))

(deftest round-trip-files
  (let [r (post-file "./test-resources/10B-file.txt")     
        f (dload-file (get-in r [:headers "Location"]))]
    (is (= 201
           (:status r)))
    (is (files-match?  "./test-resources/10B-file.txt"
                       f)))
  (let [r (post-file "./test-resources/10MB-file.txt")
        f (dload-file (get-in r [:headers "Location"]))]
    (is (= 201
           (:status r)))
    (is (files-match?  "./test-resources/10MB-file.txt"
                       f)))
  (let [r (post-file "./test-resources/300MB-file.txt")
        ^File f (dload-file (get-in r [:headers "Location"]))]
    (is (= 201
           (:status r)))
    (is (files-match?  "./test-resources/300MB-file.txt"
                       f))
    (.delete f)))

(deftest metadata-manipulation
  (let [r (post-file "./test-resources/10B-file.txt")
        m (get-metadata (extract-id r))]
    (is (= 201
           (:status r)))
    (is (= 200
           (:status m)))))
