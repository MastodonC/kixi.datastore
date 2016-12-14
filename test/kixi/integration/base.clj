(ns kixi.integration.base
  (:require [byte-streams :as bs]
            [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clojure.spec.test :as stest]
            [cheshire.core :as json]
            [environ.core :refer [env]]
            [kixi.comms :as c]
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
  [metadata-response]
  (get-in metadata-response [:body ::ms/id]))

(defn extract-id-location
  [resp]
  (when-let [locat (get-in resp [:headers "Location"])]
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

(defn file-size
  [^String file-name]
  (.length (io/file file-name)))

(def comms (atom nil))

(defn extract-comms
  [all-tests]
  (reset! comms (:communications @repl/system))
  (all-tests)
  (reset! comms nil))

(defn attach-event-handler!
  [group-id event handler]
  (c/attach-event-handler!
   @comms
   group-id
   event
   "1.0.0"
   handler))

(defn attach-create-upload-link
  [receiver]
  (attach-event-handler!
   :get-upload-link
   :kixi.datastore.filestore/upload-link-created
   #(do (reset! receiver %)
        nil)))

(defn detach-handler
  [handler]
  (c/detach-handler!
   @comms
   handler))

(defn send-upload-link-cmd
  []
  (c/send-command!
   @comms
   :kixi.datastore.filestore/create-upload-link
   "1.0.0" {}))

(defn send-metadata-cmd
  [metadata]
  (c/send-command!
   @comms
   :kixi.datastore.filestore/create-file-metadata
   "1.0.0" metadata))

(defn get-upload-link-event
  []
  (let [result (atom nil)
        handler (attach-create-upload-link
                  result)]    
    (send-upload-link-cmd)
    (wait-for-pred #(deref result))
    (detach-handler handler)
    @result))

(defn get-upload-link
  []
  (let [link-event (get-upload-link-event)]
    [(get-in link-event [:kixi.comms.event/payload :kixi.datastore.filestore/upload-link])
     (get-in link-event [:kixi.comms.event/payload :kixi.datastore.filestore/id])]))

(defmulti upload-file  
  (fn [^String target file-name]
    (subs target 0
          (.indexOf target
                    ":"))))

(defn strip-protocol
  [^String path]
  (subs path
        (+ 3 (.indexOf path
                       ":"))))

(defmethod upload-file "file"
  [target file-name]
  (io/copy (io/file file-name)
           (doto (io/file (strip-protocol target))
             (.createNewFile))))

(defn create-metadata
  ([uid file-name]
   (create-metadata uid file-name nil))  
  ([uid file-name schema-id]
   (create-metadata
    {:file-name file-name
     :type "stored"
     :sharing {::ms/file-read [uid]
               ::ms/meta-read [uid]}
     :provenance {::ms/source "upload"
                  :kixi.user/id uid}
     :size-bytes (file-size file-name)
     :schema-id schema-id
     :header true}))
  ([{:keys [^String file-name schema-id user-groups sharing header size-bytes provenance]}]
   (merge {}
          (when type
            {::ms/type "stored"})
          (when file-name
            {::ms/name file-name})
          (when-not (nil? header)
            {::ms/header header})
          (when sharing
            {::ms/sharing sharing})
          (when schema-id
            {::ss/id schema-id})
          (when provenance
            {::ms/provenance provenance})
          (when size-bytes
            {::ms/size-bytes size-bytes}))))

(defn deliver-file-and-metadata-no-wait
  [metadata]
  (let [[link id] (get-upload-link)]
    (is link)
    (is id)
    (let [md-with-id (assoc metadata ::ms/id id)]
      (when link
        (upload-file link
                     (::ms/name md-with-id))
        (send-metadata-cmd md-with-id))
      md-with-id)))

(defn deliver-file-and-metadata
  [metadata]
  (let [md-with-id (deliver-file-and-metadata-no-wait metadata)]
    (wait-for-metadata-key (get-in md-with-id [::ms/provenance :kixi.user/id])
                           (::ms/id md-with-id)
                           ::ms/id)))

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

(defn vec-if-not
  [x]
  (if (vector? x)
    x
    (vector x)))

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


(defmacro is-file-metadata-rejected
  [deliverer rejection-submap]
  `(let [receiver# (atom nil)
         rejection-handler# (attach-event-handler!
                            :file-metadata-rejections
                            :kixi.datastore.filestore/file-metadata-rejected
                            #(do (reset! receiver# %)
                                 nil))]
     (try
       (~deliverer)
       (wait-for-pred #(deref receiver#))
       (is @receiver#
           "Rejection message not received")
       (when @receiver#
         (is-submap ~rejection-submap
                    (:kixi.comms.event/payload @receiver#)))
       (finally
         (detach-handler rejection-handler#)))))
