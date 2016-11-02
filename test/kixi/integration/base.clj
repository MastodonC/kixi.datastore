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
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as ms])
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

(defn extract-id
  [file-response]
  (when-let [locat (get-in file-response [:headers "Location"])]
    (subs locat (inc (clojure.string/last-index-of locat "/")))))

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

(def accept-status #{200 201 202})

(defn wait-for-url
  ([url uid]
   (wait-for-url url uid wait-tries 1 nil))
  ([url uid tries cnt last-result]
   (if (<= cnt tries)
     (let [md (client/get url
                          {:accept :json
                           :throw-exceptions false
                           :headers {"user-groups" uid}})]
       (if (= 404 (:status md))
         (do
           (when (zero? (mod cnt every-count-tries-emit))
             (println "Waited" cnt "times for" url ". Getting:" (:status md)))
           (Thread/sleep wait-per-try)
           (recur url uid tries (inc cnt) md))
         md))
     last-result)))


(defn get-metadata
  [id ugroup]
  (update (client/get (metadata-url id) {:as :json
                                         :accept :json
                                         :throw-exceptions false
                                         :headers {"user-groups" ugroup}})
          :body
          parse-json))

(defn wait-for-metadata-key
  ([id k ugroup]
   (wait-for-metadata-key id k ugroup wait-tries 1 nil))
  ([id k ugroup tries cnt lr]
   (if (<= cnt tries)
     (let [md (get-metadata id ugroup)]
       (if-not (get-in md [:body k])
         (do
           (when (zero? (mod cnt every-count-tries-emit))
             (println "Waited" cnt "times for" k "to be metadata for" id))
           (Thread/sleep wait-per-try)
           (recur id k ugroup tries (inc cnt) md))
         md))
     (throw (Exception. (str "Metadata key never appeared: " k ". Response: " lr))))))

(defn vec-if-not
  [x]
  (if (vector? x)
    x
    (vector x)))

(defn post-file-flex
  [& {:keys [file-name schema-id user-id user-groups sharing]}]
  (check-file file-name)
  (let [r (client/post file-url
                       {:multipart [{:name "file" :content (io/file file-name)}
                                    {:name "file-metadata" 
                                     :content (encode-json (merge {:name "foo"
                                                                   :header true
                                                                   :schema-id schema-id}
                                                                  (when sharing
                                                                    {:sharing sharing})))}]
                        :headers {"user-id" user-id
                                  "user-groups" (vec-if-not user-groups)}
                        :throw-exceptions false
                        :accept :json})]
    (if-not (= 500 (:status r)) 
      (update r :body parse-json)
      (do 
        (clojure.pprint/pprint r)
        r))))

(defn post-file-and-wait
  [& {:as args}]
  (let [pfr (apply post-file-flex (flatten (seq args)))]
    (when (accept-status (:status pfr))
      (wait-for-metadata-key (extract-id pfr) ::ms/id
                             (:user-group args)))
    pfr))

(defn post-file
  [file-name schema-id id]
  (post-file-flex :file-name file-name 
                  :schema-id schema-id 
                  :user-id id
                  :user-groups id
                  :sharing {:file-read [id]
                            :meta-read [id]}))

(defn post-segmentation
  [url seg]
  (client/post url
               {:form-params seg
                :headers {"user-id" (uuid)}
                :content-type :json
                :throw-exceptions false
                :as :json}))

(defn post-spec  
  [s uid ugroup sharing]
  (client/post schema-url
               {:form-params {:schema (merge s
                                             sharing)}
                :content-type :json
                :headers {"user-id" uid
                          "user-groups" (vec-if-not ugroup)}
                :accept :json
                :throw-exceptions false}))

(defn get-spec
  [id uid]
  (wait-for-url (str schema-url id) uid))

(defn post-spec-and-wait
  ([s uid]
   (post-spec-and-wait s uid uid))
  ([s uid ugroup]
   (post-spec-and-wait s uid ugroup
                       {:sharing {:read [ugroup]
                                  :use [ugroup]}}))
  ([s uid ugroup sharing]
   (let [psr (post-spec s uid ugroup sharing)]
     (when (accept-status (:status psr))
       (wait-for-url (get-in psr [:headers "Location"]) uid))
     psr)))

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

(defn get-file
  [id uid ugroups]
  (client/get (str file-url "/" id) 
              {:headers (apply merge {"user-id" uid}
                               (map #(hash-map "user-groups" %) (vec-if-not ugroups)))
               :throw-exceptions false}))

(defn dload-file
  [location uid]
  (let [_ (wait-for-url location uid)
        f (java.io.File/createTempFile (uuid) ".tmp")
        _ (.deleteOnExit f)]
    (bs/transfer (:body (client/get location {:as :stream
                                              :headers {"user-groups" uid}}))
                 f)
    f))

(defn dload-file-by-id
  [id uid]
  (dload-file (str file-url "/" id) uid))


(defmacro when-status
  [status resp rest]
  `(let [rs# (:status ~resp)]
     (is-submap {:status ~status}
                ~resp)
     (when (= ~status
              rs#)
       ~@rest)))

(defmacro when-accepted
  [resp & rest]
  `(when-status 202 ~resp ~rest))

(defmacro when-created
  [resp & rest]
  `(when-status 201 ~resp ~rest))
