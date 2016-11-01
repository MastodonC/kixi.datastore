(ns kixi.integration.base
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clojure.spec.test :as stest]
            [cheshire.core :as json]
            [environ.core :refer [env]]
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

(def wait-tries (Integer/parseInt (env :wait-tries "80")))
(def wait-per-try (Integer/parseInt (env :wait-per-try "100")))
(def wait-emit-msg (Integer/parseInt (env :wait-emit-msg "5000")))

(def every-count-tries-emit (int (/ wait-emit-msg wait-per-try)))

(defn uuid 
  []
  (str (java.util.UUID/randomUUID)))

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
  (stest/instrument ['kixi.datastore.web-server/return-error
                     'kixi.datastore.metadatastore.inmemory/update-metadata-processor
                     'kixi.datastore.communication-specs/send-event!]))

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
  (if (string? s)
    (json/parse-string s keyword)
    s))

(defn encode-json
  [m]
  (json/encode m))

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

(def accept-status #{200 201})

(defn post-file
  [file-name schema-id]
  (check-file file-name)
  (let [r (client/post file-url
                       {:multipart [{:name "file" :content (io/file file-name)}
                                    {:name "file-metadata" :content (encode-json {:name "foo"
                                                                                  :header true
                                                                                  :schema-id schema-id
                                                                                  :user-id (uuid)
                                                                                  :file-sharing {:read [(uuid)]}
                                                                                  :file-metadata-sharing {:update [(uuid)]}})}]
                        :throw-exceptions false
                        :accept :json})]
    (if-not (= 500 (:status r)) 
      (update r :body parse-json)
      (do 
        (clojure.pprint/pprint r)
        r))))

(defn post-segmentation
  [url seg]
  (client/post url
               {:form-params seg
                :headers {"user-id" (uuid)}
                :content-type :json
                :throw-exceptions false
                :as :json}))

(defn post-spec
  [s]
  (client/post schema-url
               {:form-params {:schema s}
                :content-type :json
                :accept :json
                :throw-exceptions false}))

(defn wait-for-url
  ([url]
   (wait-for-url url wait-tries 1 nil))
  ([url tries cnt last-result]
   (if (<= cnt tries)
     (let [md (client/get url
                          {:accept :json
                           :throw-exceptions false})]
       (if (= 404 (:status md))
         (do
           (when (zero? (mod cnt every-count-tries-emit))
             (println "Waited" cnt "times for" url ". Getting:" (:status md)))
           (Thread/sleep wait-per-try)
           (recur url tries (inc cnt) md))
         md))
     last-result)))

(defn get-spec
  [id]
  (wait-for-url (str schema-url id)))

(defn get-spec-direct
  [id]
  (client/get (str schema-url id)
              {:accept :json
               :throw-exceptions false}))

(defn extract-schema
  [r-g]
  (when (= 200 (:status r-g))
    (-> r-g
        :body
        parse-json)))

(defn dload-file
  [location]
  (let [_ (wait-for-url location)
        f (java.io.File/createTempFile (uuid) ".tmp")
        _ (.deleteOnExit f)]
    (bs/transfer (:body (client/get location {:as :stream}))
                 f)
    f))

(defn dload-file-by-id
  [id]
  (dload-file (str file-url "/" id)))

(defn get-metadata
  [id]
  (update (client/get (metadata-url id) {:as :json
                                         :accept :json
                                         :throw-exceptions false})
          :body
          parse-json))

(defn wait-for-metadata-key
  ([id k]
   (wait-for-metadata-key id k wait-tries 1 nil))
  ([id k tries cnt lr]
   (if (<= cnt tries)
     (let [md (get-metadata id)]
       (if-not (get-in md [:body k])
         (do
           (when (zero? (mod cnt every-count-tries-emit))
             (println "Waited" cnt "times for" k "to be metadata for" id))
           (Thread/sleep wait-per-try)
           (recur id k tries (inc cnt) md))
         md))
     (throw (Exception. (str "Metadata key never appeared: " k ". Response: " lr))))))

(defn extract-id
  [file-response]
  (when-let [locat (get-in file-response [:headers "Location"])]
    (subs locat (inc (clojure.string/last-index-of locat "/")))))
