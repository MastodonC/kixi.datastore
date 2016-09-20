(ns kixi.integration.upload-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [digest :as d]
            [kixi.integration.base :refer [service-url cycle-system-fixture uuid]])
  (:import [java.io 
            File
            FileNotFoundException]))

(use-fixtures :once cycle-system-fixture)

(def file-url (str "http://" (service-url) "/file"))

(defn metadata-url
  [id]
  (str file-url "/" id "/meta"))

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
                                 :accept :json
                                 :throw-exceptions false}))

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

(defn wait-for-metadata-key
  ([id k]
   (wait-for-metadata-key id k 10))
  ([id k tries]
   (when (pos? tries)
     (Thread/sleep 500)
     (let [md (get-metadata id)]
       (if-not (get-in md [:body k])
         (recur id k (dec tries))
         md)))))

(deftest wait-for-schema-works
  (let [r (post-file "./test-resources/10B-file.txt")
        m (wait-for-metadata-key (extract-id r) :schema)]
    (is (= 201
           (:status r)))
    (is (= 200
           (:status m)))
    (is (get-in m [:body :schema]))
    (is (get-in m [:body :id]))))
