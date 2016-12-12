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
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           [only-in-ex# only-in-ac# shared#] (clojure.data/diff exp# act#)]
       (if only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing expected elements.")
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
                     'kixi.datastore.communication-specs/send-event!
                     'kixi.datastore.transport-specs/filemetadata-transport->internal
                     'kixi.datastore.transport-specs/schema-transport->internal]))

(defn cycle-system-fixture
  [all-tests]
  (repl/start)
  (try (instrument-specd-functions)
       (all-tests)
       (finally
         (repl/stop))))

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

(defn file-url 
  ([] 
   (str "http://" (service-url) "/file"))
  ([id]
   (str (file-url) "/" id)))

(defn metadata-url
  [id]
  (str (file-url) "/" id "/meta"))

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
  ([uid url]
   (wait-for-url uid url wait-tries 1 nil))
  ([uid url tries cnt last-result]
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
           (recur uid url tries (inc cnt) md))
         md))
     last-result)))


(defn get-metadata
  [ugroup id]
  (update (client/get (metadata-url id)
                      {:as :json
                       :accept :json
                       :throw-exceptions false
                       :headers {"user-groups" ugroup}})
          :body
          parse-json))

(defn wait-for-metadata-key
  ([ugroup id k]
   (wait-for-metadata-key ugroup id k wait-tries 1 nil))
  ([ugroup id k tries cnt lr]
   (if (<= cnt tries)
     (let [md (get-metadata ugroup id)]
       (if-not (get-in md [:body k])
         (do
           (when (zero? (mod cnt every-count-tries-emit))
             (println "Waited" cnt "times for" k "to be in metadata for" id ". Getting: " md))
           (Thread/sleep wait-per-try)
           (recur ugroup id k tries (inc cnt) md))
         md))
     (throw (Exception. (str "Metadata key never appeared: " k ". Response: " lr))))))

(defn vec-if-not
  [x]
  (if (vector? x)
    x
    (vector x)))

(defn file-size
  [^String file-name]
  (.length (io/file file-name)))

(defn post-file-no-wait
  [{:keys [^String file-name schema-id user-id user-groups sharing header file-size]}]
  (check-file file-name)
  (let [r (client/post file-url
                       {:multipart [{:name "file"
                                     :content (io/file file-name)}
                                    {:name "file-metadata"
                                     :content (encode-json (merge (when file-name
                                                                    {:name file-name})
                                                                  (if-not (nil? header)
                                                                    {:header header})
                                                                  (when sharing
                                                                    {:sharing sharing})
                                                                  (when schema-id
                                                                    {:schema-id schema-id})))}]
                        :headers (merge {}
                                        (when user-id
                                          {"user-id" user-id})
                                        (when user-groups
                                          {"user-groups" (vec-if-not user-groups)})
                                        (when file-size
                                          {"file-size" file-size}))
                        :throw-exceptions false
                        :accept :json})]
    (if-not (= 500 (:status r))
      (update r :body parse-json)
      (do
        (clojure.pprint/pprint r)
        r))))

(defn post-file
  ([uid file-name]
   (post-file uid uid file-name nil))
  ([uid file-name schema-id]
   (post-file uid uid file-name schema-id))
  ([uid ugroup file-name schema-id]
   (post-file {:file-name file-name
               :schema-id schema-id
               :user-id uid
               :user-groups ugroup
               :sharing {:file-read [ugroup]
                         :meta-read [ugroup]}
               :file-size (file-size file-name)
               :header true}))
  ([{:keys [file-name schema-id user-id user-groups sharing header file-size]
     :as args}]
   (let [pfr (post-file-no-wait args)]
     (when (accept-status (:status pfr))
       (wait-for-metadata-key (:user-groups args)
                              (extract-id pfr)
                              ::ms/id))
     pfr)))

(defn post-segmentation
  [url seg]
  (client/post url
               {:form-params seg
                :headers {"user-id" (uuid)}
                :content-type :json
                :throw-exceptions false
                :as :json}))

(defn get-spec
  [ugroup id]
  (client/get (str schema-url id)
              {:accept :json
               :headers {"user-groups" ugroup}
               :throw-exceptions false}))

(defn post-spec-no-wait
  ([uid s]
   (post-spec-no-wait uid uid s))
  ([uid ugroup s]
   (post-spec-no-wait uid ugroup s {:sharing {:read [ugroup]
                                              :use [ugroup]}}))
  ([uid ugroup s sharing]
   (client/post schema-url
                {:form-params (merge s
                                     sharing)
                 :content-type :json
                 :headers {"user-id" uid
                           "user-groups" (vec-if-not ugroup)}
                 :accept :json
                 :throw-exceptions false})))

(defn post-spec
  ([uid s]
   (post-spec uid uid s))
  ([uid ugroup s]
   (post-spec uid ugroup s
                       {:sharing {:read [ugroup]
                                  :use [ugroup]}}))
  ([uid ugroup s sharing]
   (let [psr (post-spec-no-wait uid ugroup s sharing)]
     (when (accept-status (:status psr))
       (wait-for-url ugroup (get-in psr [:headers "Location"])))
     psr)))

(defn extract-schema
  [r-g]
  (when (= 200 (:status r-g))
    (-> r-g
        :body
        parse-json)))

(defn get-file
  [uid ugroups id]
  (client/get (str file-url "/" id)
              {:headers (apply merge {"user-id" uid}
                               (map #(hash-map "user-groups" %) (vec-if-not ugroups)))
               :throw-exceptions false}))

(defn dload-file
  [uid location]
  (let [_ (wait-for-url uid location)
        f (java.io.File/createTempFile (uuid) ".tmp")
        _ (.deleteOnExit f)]
    (bs/transfer (:body (client/get location {:as :stream
                                              :headers {"user-groups" uid}}))
                 f)
    f))

(defn dload-file-by-id
  [uid id]
  (dload-file uid (str file-url "/" id)))

(defmacro has-status
  [status resp]
  `(let [parsed# (if (= "application/json" (get-in ~resp [:headers "Content-Type"]))
                   (update ~resp :body parse-json)
                   ~resp)]
     (is-submap {:status ~status}
                parsed#)))

(defmacro success
  [resp]
  `(has-status 200
               ~resp))

(defmacro created
  [resp]
 `(has-status 201
              ~resp))

(defmacro accepted
  [resp]
  `(has-status 202
               ~resp))

(defmacro not-found
  [resp]
  `(has-status 404
               ~resp))

(defmacro bad-request
  [resp]
  `(has-status 400
               ~resp))

(defmacro unauthorised
  [resp]
  `(has-status 401
               ~resp))

(defmacro when-status
  [status resp rest]
  `(let [rs# (:status ~resp)]
     (has-status ~status ~resp)
     (when (= ~status
              rs#)
       ~@rest)))

(defmacro when-accepted
  [resp & rest]
  `(when-status 202 ~resp ~rest))

(defmacro when-created
  [resp & rest]
  `(when-status 201 ~resp ~rest))

(defmacro when-success
  [resp & rest]
  `(when-status 200 ~resp ~rest))

(defn wait-for-pred
  ([p]
   (wait-for-pred p wait-tries))
  ([p tries]
   (wait-for-pred p tries wait-per-try))
  ([p tries ms]
   (loop [try tries]
     (when (and (pos? try) (not (p)))
       (Thread/sleep ms)
       (recur (dec try))))))
