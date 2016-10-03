(ns kixi.integration.base
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [cheshire.core :as json]
            [kixi.repl :as repl]
            [kixi.datastore.transit :as t]
            [clj-http.client :as client]
            [clojure.data]
            [clojure.java.io :as io]
            [digest :as d])
  (:import [java.io 
            File
            FileNotFoundException]))

(defmacro is-submap
  [expected actual]
  `(try
     (let [act# ~actual
           exp# ~expected
           [only-in-ex# only-in-ac# shared#] (clojure.data/diff exp# act#)]
       (if only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message "Missing expected elements."
                                  :expected only-in-ex# :actual act#})
         (clojure.test/do-report {:type :pass
                                  :message "Matched"
                                  :expected exp# :actual act#})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception diffing"
                   :expected nil :actual t#}))))

(defn cycle-system-fixture
  [all-tests]
  (repl/start)
  (all-tests)
  (repl/stop))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defmacro deftest-broken
  [name & everything]
  `(clojure.test/deftest ~(vary-meta name assoc :integration true) ~@everything))

(defn service-url
  []
  (or (System/getenv "SERVICE_URL") "localhost:8080"))


(def schema-url (str "http://" (service-url) "/schema/"))


(defn parse-json
  [s]
  (json/parse-string s))

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


(defn files-match?
  [^String one ^File two]
  (= (d/md5 (File. one))
     (d/md5 two)))

(defn post-file
  [file-name schema-name]
  (check-file file-name)
  (client/post file-url
               {:multipart [{:name "file" :content (io/file file-name)}
                            {:name "name" :content "foo"}
                            {:name "schema-name" :content schema-name}]
                :throw-exceptions false
                :accept :json}))

(defn post-segmentation
  [url seg]
  (client/post url               
               {:form-params seg
                :content-type :json
                :throw-exceptions false
                :as :json}))

(defn post-spec
  [n s]
  (client/post (str schema-url (name n))
               {:form-params {:definition s}
                :content-type :transit+json
                :transit-opts {:encode t/write-handlers
                               :decode t/read-handlers}}))

(defn get-spec
  [n]
  (client/get (str schema-url (name n))
              {:accept :transit+json
               :as :stream
               :throw-exceptions false}))

(defn extract-spec
  [r-g]
  (when (= 200 (:status r-g))
    (-> r-g
        :body
        (client/parse-transit :json {:decode t/read-handlers})
        :definition)))

(defn dload-file
  [location]
  (let [f (java.io.File/createTempFile (uuid) ".tmp")
        _ (.deleteOnExit f)]
    (bs/transfer (:body (client/get location {:as :stream}))
                 f)
    f))

(defn dload-file-by-id
  [id]
  (dload-file (str file-url "/" id)))

(defn get-metadata
  [id]
  (client/get (metadata-url id) {:as :json
                                 :accept :json
                                 :throw-exceptions false}))

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

(defn extract-id
  [file-response]
  (when-let [locat (get-in file-response [:headers "Location"])]
    (subs locat (inc (clojure.string/last-index-of locat "/")))))



