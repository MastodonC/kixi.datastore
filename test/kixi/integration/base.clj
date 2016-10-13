(ns kixi.integration.base
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clojure.spec.test :as stest]
            [cheshire.core :as json]
            [kixi.repl :as repl]
            [kixi.datastore.transit :as t]
            [clj-http.client :as client]
            [clojure.data]
            [clojure.java.io :as io]
            [digest :as d]
            [kixi.datastore.schemastore :as ss])
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

(defn instrument-specd-functions
  []
  (stest/instrument 'kixi.datastore.web-server/return-error))

(defn cycle-system-fixture
  [all-tests]
  (repl/start)
  (instrument-specd-functions)
  (all-tests)
  (repl/stop))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn nskw->str
  [nskw]
  (subs (str nskw) 1))


(defmacro deftest-broken
  [name & everything]
  `(clojure.test/deftest ~(vary-meta name assoc :integration true) ~@everything))

(defn service-url
  []
  (or (System/getenv "SERVICE_URL") "localhost:8080"))


(def schema-url (str "http://" (service-url) "/schema/"))


(defn parse-json
  [s]
  (json/parse-string s keyword))

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
  [file-name schema-id]
  (check-file file-name)
  (update (client/post file-url
                       {:multipart [{:name "file" :content (io/file file-name)}
                                    {:name "name" :content "foo"}
                                    {:name "schema-id" :content schema-id}]
                        :throw-exceptions false
                        :accept :json})
          :body parse-json))

(defn post-segmentation
  [url seg]
  (client/post url
               {:form-params seg
                :content-type :json
                :throw-exceptions false
                :as :json}))

(defn post-spec
  [s]
  (client/post schema-url
               {:form-params {:schema s}
                :content-type :transit+json
                :accept :transit+json
                :throw-exceptions false
                :transit-opts {:encode t/write-handlers
                               :decode t/read-handlers}}))
(defn wait-for-url
  ([url]
   (wait-for-url url 10))
  ([url tries]
   (when (pos? tries)
     (Thread/sleep 500)
     (let [md (client/get url
                          {:accept :transit+json
                           :as :stream
                           :throw-exceptions false
                           :transit-opts {:encode t/write-handlers
                                          :decode t/read-handlers}})]
       (if (= 404 (:status md))
         (recur url (dec tries))
         md)))))

(defn get-spec
  [id]
  (client/get (str schema-url id)
              {:accept :transit+json
               :as :stream
               :throw-exceptions false
               :transit-opts {:encode t/write-handlers
                              :decode t/read-handlers}}))

(defn extract-schema
  [r-g]
  (when (= 200 (:status r-g))
    (-> r-g
        :body
        (client/parse-transit :json {:decode t/read-handlers}))))

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
   (if (pos? tries)
     (do (Thread/sleep 500)
         (let [md (get-metadata id)]
           (if-not (get-in md [:body k])
             (recur id k (dec tries))
             md)))
     (throw (Exception. "Metadata key never appeared.'")))))

(defn extract-id
  [file-response]
  (when-let [locat (get-in file-response [:headers "Location"])]
    (subs locat (inc (clojure.string/last-index-of locat "/")))))
