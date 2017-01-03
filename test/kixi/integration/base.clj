(ns kixi.integration.base
  (:require [byte-streams :as bs]
            [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure data 
             [test :refer :all]]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.spec.test :as stest]
            [digest :as d]
            [environ.core :refer [env]]
            [kixi
             [comms :as c]
             [repl :as repl]]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]])
  (:import [java.io File FileNotFoundException]))

(def wait-tries (Integer/parseInt (env :wait-tries "80")))
(def wait-per-try (Integer/parseInt (env :wait-per-try "100")))
(def wait-emit-msg (Integer/parseInt (env :wait-emit-msg "5000")))

(def every-count-tries-emit (int (/ wait-emit-msg wait-per-try)))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn vec-if-not
  [x]
  (if (vector? x)
    x
    (vector x)))

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
                     'kixi.datastore.transport-specs/schema-transport->internal
                     'kixi.datastore.metadatastore.elasticsearch/query-criteria->es-query]))

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


(defn schema-url
  ([]
   (str "http://" (service-url) "/schema/"))
  ([id]
   (str (schema-url) id)))

(defn metadata-query-url
  []
   (str "http://" (service-url) "/metadata"))

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
                           :headers {"user-groups" (vec-if-not uid)}})]
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
                       :headers {"user-groups" (vec-if-not ugroup)}})
          :body
          parse-json))

(defn encode-kw
  [kw]
  (str (namespace kw)
       "_"
       (name kw)))

(defn search-metadata
  ([group-ids activities]
   (search-metadata group-ids activities nil nil))
  ([group-ids activities index count]
   (update (client/get (metadata-query-url)
                       {:query-params (merge (zipmap (repeat :activity)
                                                     (map encode-kw activities))
                                             (when index
                                               {:index index})
                                             (when count
                                               {:count count}))
                        :accept :json
                        :throw-exceptions false
                        :headers {"user-groups" (vec-if-not group-ids)}})
           :body
           parse-json)))

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
  ([uid]
   (send-upload-link-cmd uid uid))
  ([uid ugroup]
   (c/send-command!
    @comms
    :kixi.datastore.filestore/create-upload-link
    "1.0.0" 
    {:kixi.user/id uid
     :kixi.user/groups (vec-if-not ugroup)}
    {})))

(defn send-metadata-cmd
  ([uid metadata]
   (send-metadata-cmd uid uid metadata))
  ([uid ugroup metadata]
   (c/send-command!
    @comms
    :kixi.datastore.filestore/create-file-metadata
    "1.0.0" 
    {:kixi.user/id uid
     :kixi.user/groups (vec-if-not ugroup)}
    metadata)))

(defn send-spec-no-wait
  ([uid spec]
   (send-spec-no-wait uid uid spec))
  ([uid ugroup spec]
   (c/send-command!
    @comms
    :kixi.datastore.schema/create
    "1.0.0"
    {:kixi.user/id uid
     :kixi.user/groups (vec-if-not ugroup)}
    spec)))

(defn wait-for-atoms
  [& atoms]
  (first
   (async/alts!!
    (mapv (fn [a]
            (async/go
              (wait-for-pred #(deref a))
              @a))
          atoms))))

(defn send-spec
  [uid spec]
  (let [rejected-a (atom nil)
        created-a (atom nil)
        rejection-handler (attach-event-handler!
                           :send-spec-rejections
                           :kixi.datastore.schema/rejected
                           #(do (when (= uid
                                         (get-in % [:kixi.comms.event/payload :schema ::ss/provenance :kixi.user/id]))
                                  (reset! rejected-a %))
                                nil))
        success-handler (attach-event-handler!
                         :send-spec-successes
                         :kixi.datastore.schema/created
                         #(do (when (= uid
                                       (get-in % [:kixi.comms.event/payload ::ss/provenance :kixi.user/id]))
                                (reset! created-a %))
                              nil))]
    (try
      (send-spec-no-wait uid spec)
      (let [event (wait-for-atoms rejected-a created-a)]
        (if (= :kixi.datastore.schema/created
               (:kixi.comms.event/key  event))
          (wait-for-url uid (schema-url
                             (get-in event [:kixi.comms.event/payload ::ss/id])))
          event))
      (finally
        (detach-handler rejection-handler)
        (detach-handler success-handler)))))

(defn metadata->user-id
  [metadata]
  (get-in metadata [::ms/provenance :kixi.user/id]))

(defn get-upload-link-event
  [user-id]
  (let [result (atom nil)
        handler (attach-create-upload-link
                 result)]
    (send-upload-link-cmd user-id)
    (wait-for-pred #(deref result))
    (detach-handler handler)
    @result))

(defn get-upload-link
  [user-id]
  (let [link-event (get-upload-link-event user-id)]
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

(defmethod upload-file "https"
  [target file-name]
  (client/put target
              {:body (io/file file-name)}))

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

(defn send-file-and-metadata-no-wait
  ([metadata]
   (send-file-and-metadata-no-wait
    (metadata->user-id metadata)
    (metadata->user-id metadata)
    metadata))
  ([uid ugroup metadata]
   (let [[link id] (get-upload-link ugroup)]
     (is link)
     (is id)
     (let [md-with-id (assoc metadata ::ms/id id)]
       (when link
         (upload-file link
                      (::ms/name md-with-id))
         (send-metadata-cmd ugroup
                            md-with-id))
       md-with-id))))

(defn send-file-and-metadata
  ([metadata]
   (send-file-and-metadata
    (metadata->user-id metadata)
    (metadata->user-id metadata)
    metadata))
  ([uid ugroup metadata]
   (let [rejected-a (atom nil)
         created-a (atom nil)
         rejection-handler (attach-event-handler!
                            :send-file-metadata-rejections
                            :kixi.datastore.file-metadata/rejected
                            #(do (when (= uid
                                          (get-in % [:kixi.comms.event/payload ::ms/file-metadata ::ms/provenance :kixi.user/id]))
                                   (reset! rejected-a %))
                                 nil))
         success-handler (attach-event-handler!
                          :send-file-metadata-sucesses
                          :kixi.datastore.file-metadata/updated
                          #(do (when (= uid
                                        (get-in % [:kixi.comms.event/payload ::ms/file-metadata ::ms/provenance :kixi.user/id]))
                                 (reset! created-a %))
                               nil))]
     (try
       (send-file-and-metadata-no-wait uid ugroup metadata)
       (let [event (wait-for-atoms rejected-a created-a)]
         (if (= :kixi.datastore.file-metadata/updated
                (:kixi.comms.event/key event))
           (wait-for-metadata-key ugroup
                                  (get-in event [:kixi.comms.event/payload
                                                 ::ms/file-metadata
                                                 ::ms/id])
                                  ::ms/id)
           event))
       (finally
         (detach-handler rejection-handler)
         (detach-handler success-handler))))))

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
  (client/get (schema-url id)
              {:accept :json
               :headers {"user-groups" ugroup}
               :throw-exceptions false}))

(defn extract-schema
  [r-g]
  (when (= 200 (:status r-g))
    (-> r-g
        :body
        parse-json)))

(defn get-file
  [uid ugroups id]
  (client/get (file-url id)
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
  (dload-file uid (file-url id)))

(defn schema->schema-id
  [schema]
  (let [r (send-spec (get-in schema [::ss/provenance :kixi.user/id]) schema)]
    (if (= 200 (:status r))
      (::ss/id (extract-schema r))
      (throw (Exception. (str "Couldn't post small-segmentable-file-schema. Resp: " r))))))

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
                             :send-file-metadata-rejections
                             :kixi.datastore.file-metadata/rejected
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
