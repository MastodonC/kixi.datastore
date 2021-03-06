(ns kixi.integration.base
  (:require [amazonica.aws.dynamodbv2 :as ddb]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure data
             [test :refer :all]]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.spec.test.alpha :as stest]
            [kixi.datastore.schemastore.utils :as sh]
            [digest :as d]
            [environ.core :refer [env]]
            [kixi.comms :as c]
            [kixi.comms.components.kinesis :as kinesis]
            [kixi.datastore
             [application :as app]
             [filestore :as fs]
             [metadatastore :as ms]
             [schemastore :as ss]]
            [user :as user]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.filestore
             [upload :as fsupload]]
            [taoensso.timbre :as log])
  (:import [java.io File FileNotFoundException]))

(sh/alias 'cmd 'kixi.command)
(sh/alias 'event 'kixi.event)

(def wait-tries (Integer/parseInt (env :wait-tries "3000")))
(def wait-per-try (Integer/parseInt (env :wait-per-try "10")))
(def wait-emit-msg (Integer/parseInt (env :wait-emit-msg "5000")))
(def run-against-staging (Boolean/parseBoolean (env :run-against-staging "false")))

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

(defmacro is-match
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           [only-in-ac# only-in-ex# shared#] (clojure.data/diff act# exp#)]
       (cond
         only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing expected elements.")
                                  :expected only-in-ex# :actual act#})
         only-in-ac#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Has extra elements.")
                                  :expected {} :actual only-in-ac#})
         :else (clojure.test/do-report {:type :pass
                                        :message "Matched"
                                        :expected exp# :actual act#})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception diffing"
                                :expected ~expected :actual t#}))))

(defn instrument-specd-functions
  []
  (stest/instrument ['kixi.datastore.web-server/return-error
                     'kixi.datastore.metadatastore.inmemory/update-metadata-processor
                     'kixi.datastore.communication-specs/send-event!
                     'kixi.datastore.transport-specs/filemetadata-transport->internal
                     'kixi.datastore.transport-specs/schema-transport->internal
                     'kixi.datastore.metadatastore.elasticsearch/query-criteria->es-query]))

(defn table-exists?
  [endpoint table]
  (try
    (ddb/describe-table {:endpoint endpoint} table)
    (catch Exception e false)))

(defn delete-tables
  [endpoint table-names]
  (doseq [sub-table-names (partition-all 10 table-names)]
    (doseq [table-name sub-table-names]
      (ddb/delete-table {:endpoint endpoint} :table-name table-name))
    (loop [tables sub-table-names]
      (when (not-empty tables)
        (recur (doall (filter (partial table-exists? endpoint) tables)))))))

(defn tear-down-kinesis
  [{:keys [endpoint dynamodb-endpoint streams
           profile app teardown-kinesis teardown-dynamodb]}]
  (when teardown-dynamodb
    (delete-tables dynamodb-endpoint [(kinesis/event-worker-app-name app profile)
                                      (kinesis/command-worker-app-name app profile)]))
  (when teardown-kinesis
    (kinesis/delete-streams! endpoint (vals streams))))

(defn cycle-system-fixture
  [all-tests & [components]]
  (cond
    (not-empty components)
    (user/start {} components)

    run-against-staging
    (user/start {} [:communications])

    :else
    (user/start))
  (try (instrument-specd-functions)
       (all-tests)
       (finally
         (let [kinesis-conf (select-keys (:communications @app/system)
                                         [:endpoint :dynamodb-endpoint :streams
                                          :profile :app :teardown-kinesis :teardown-dynamodb])]
           (user/stop)
           (tear-down-kinesis kinesis-conf)))))

(defn create-cycle-system-fixture
  [components]
  (fn [all-tests]
    (cycle-system-fixture all-tests components)))

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
  (env :service-url "localhost:8080"))


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

(defn file-download-url
  ([]
   (str "http://" (service-url) "/file"))
  ([id]
   (str (file-url) "/" id "/link")))

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
                          {:accept :transit+json
                           :as :transit+json
                           :throw-exceptions false
                           :headers {:user-id uid
                                     :user-groups (vec-if-not uid)}})]
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
  (client/get (metadata-url id)
              {:as :transit+json
               :accept :transit+json
               :throw-exceptions false
               :headers {"user-id" (uuid)
                         "user-groups" (vec-if-not ugroup)}}))

(defn encode-kw
  [kw]
  (str (namespace kw)
       "_"
       (name kw)))

(defn wait-for-pred
  ([p]
   (wait-for-pred p wait-tries))
  ([p tries]
   (wait-for-pred p tries wait-per-try))
  ([p tries ms]
   (loop [try tries]
     (when (and (pos? try))
       (let [result (p)]
         (if (not result)
           (do
             (Thread/sleep ms)
             (recur (dec try)))
           result))))))

(defn search-metadata
  ([group-ids activities]
   (search-metadata group-ids activities nil nil))
  ([group-ids activities index]
   (search-metadata group-ids activities index nil nil))
  ([group-ids activities index count]
   (search-metadata group-ids activities index count nil))
  ([group-ids activities index count order]
   (client/get (metadata-query-url)
               {:query-params (merge {:activity (map encode-kw activities)}
                                     (when index
                                       {:index index})
                                     (when count
                                       {:count count})
                                     (when order
                                       {:sort-order order}))
                :accept :transit+json
                :as :transit+json
                :throw-exceptions false
                :coerce :always
                :headers {"user-id" (uuid)
                          "user-groups" (vec-if-not group-ids)}})))

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

(defn wait-for-metadata-to-be-searchable
  ([ugroup id]
   (wait-for-metadata-to-be-searchable ugroup id wait-tries 1 nil))
  ([ugroup id tries cnt lr]
   (if (<= cnt tries)
     (let [md (wait-for-metadata-key ugroup id ::md/id)
           search-resp (search-metadata ugroup [::md/meta-read])]
       (if-not (first (get-in search-resp
                              [:body :items]))
         (do
           (when (zero? (mod cnt every-count-tries-emit))
             (println "Waited" cnt "times for " id " to be searchable using: " (keys (get-in md [:body ::md/sharing])) ". Getting: " search-resp))
           (Thread/sleep wait-per-try)
           (recur ugroup id tries (inc cnt) md))
         md))
     (throw (Exception. (str "Search never worked for: " id ". Response: " lr))))))

(defn file-size
  [^String file-name]
  (.length (io/file file-name)))

(def comms (atom nil))
(def event-channel (atom nil))

(defn attach-event-handler!
  [group-id event handler]
  (c/attach-event-handler!
   @comms
   group-id
   event
   "1.0.0"
   handler))

(defn detach-handler
  [handler]
  (c/detach-handler!
   @comms
   handler))

(defn sink-to
  [a]
  #(do (when @a
         (async/>!! @a %))
       nil))

(defn extract-comms
  [all-tests]
  (reset! comms (:communications @app/system))
  (let [_ (reset! event-channel (async/chan 100))
        handler (c/attach-event-with-key-handler!
                 @comms
                 :datastore-integration-tests
                 :kixi.comms.event/id
                 (sink-to event-channel))
        handler2 (c/attach-event-with-key-handler!
                  @comms
                  :datastore-integration-tests-new-format
                  :kixi.event/type
                  (sink-to event-channel))]
    (try
      (all-tests)
      (finally
        (detach-handler handler)
        (detach-handler handler2)
        (async/close! @event-channel)
        (reset! event-channel nil))))
  (reset! comms nil))

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
    {}
    {:kixi.comms.command/partition-key uid})))

(defn trim-file-name
  [md]
  (update md
          ::ms/name
          #(subs %
                 (inc (or (clojure.string/last-index-of % "/") -1))
                 (or (clojure.string/last-index-of % ".") (count %)))))

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
    (trim-file-name metadata)
    {:kixi.comms.command/partition-key uid})))

(defn send-file-delete-cmd
  ([uid meta-id]
   (send-file-delete-cmd uid uid meta-id))
  ([uid ugroup meta-id]
   (c/send-valid-command!
    @comms
    {::cmd/type :kixi.datastore/delete-file
     ::cmd/version "1.0.0"
     :kixi/user {:kixi.user/id uid
                 :kixi.user/groups (vec-if-not ugroup)}
     ::cmd/id (uuid)
     ::ms/id meta-id}
    {:partition-key meta-id})))

(defn send-bundle-delete-cmd
  ([uid meta-id]
   (send-bundle-delete-cmd uid uid meta-id))
  ([uid ugroup meta-id]
   (c/send-valid-command!
    @comms
    {::cmd/type :kixi.datastore/delete-bundle
     ::cmd/version "1.0.0"
     :kixi/user {:kixi.user/id uid
                 :kixi.user/groups (vec-if-not ugroup)}
     ::cmd/id (uuid)
     ::ms/id meta-id}
    {:partition-key meta-id})))

(defn send-add-files-to-bundle-cmd
  ([uid id bundled-ids]
   (send-add-files-to-bundle-cmd uid uid id bundled-ids))
  ([uid ugroup id bundled-ids]
   (c/send-valid-command!
    @comms
    {::cmd/type :kixi.datastore/add-files-to-bundle
     ::cmd/version "1.0.0"
     :kixi/user {:kixi.user/id uid
                 :kixi.user/groups (vec-if-not ugroup)}
     ::cmd/id (uuid)
     ::ms/id id
     ::ms/bundled-ids bundled-ids}
    {:partition-key "a"})))

(defn send-remove-files-from-bundle-cmd
  ([uid id bundled-ids]
   (send-remove-files-from-bundle-cmd uid uid id bundled-ids))
  ([uid ugroup id bundled-ids]
   (c/send-valid-command!
    @comms
    {::cmd/type :kixi.datastore/remove-files-from-bundle
     ::cmd/version "1.0.0"
     :kixi/user {:kixi.user/id uid
                 :kixi.user/groups (vec-if-not ugroup)}
     ::cmd/id (uuid)
     ::ms/id id
     ::ms/bundled-ids bundled-ids}
    {:partition-key "a"})))

(defn send-datapack-cmd
  ([uid metadata]
   (send-datapack-cmd uid uid metadata))
  ([uid ugroup metadata]
   (c/send-command!
    @comms
    :kixi.datastore/create-datapack
    "1.0.0"
    {:kixi.user/id uid
     :kixi.user/groups (vec-if-not ugroup)}
    (trim-file-name metadata)
    {:kixi.comms.command/partition-key uid})))

(defn send-metadata-sharing-change-cmd-old
  ([uid metadata-id change-type activity target-group]
   (send-metadata-sharing-change-cmd-old uid uid metadata-id change-type activity target-group))
  ([uid ugroup metadata-id change-type activity target-group]
   (c/send-command!
    @comms
    :kixi.datastore.metadatastore/sharing-change
    "1.0.0"
    {:kixi.user/id uid
     :kixi.user/groups (vec-if-not ugroup)}
    {::ms/id metadata-id
     ::ms/sharing-update change-type
     ::ms/activity activity
     :kixi.group/id target-group}
    {:kixi.comms.command/partition-key uid})))

(defn send-metadata-sharing-change-cmd
  ([uid metadata-id change-type activity target-group]
   (send-metadata-cmd uid uid metadata-id change-type activity target-group))
  ([uid ugroup metadata-id change-type activity target-group]
   (c/send-valid-command!
    @comms
    {:kixi.command/type :kixi.datastore/sharing-change
     :kixi.command/version "2.0.0"
     :kixi/user {:kixi.user/id uid
                 :kixi.user/groups (vec-if-not ugroup)}
     ::ms/id metadata-id
     ::ms/sharing-update change-type
     ::ms/activity activity
     :kixi.group/id target-group}
    {:partition-key uid})))

(defn send-metadata-update-cmd
  ([uid metadata-id new-metadata]
   (send-metadata-update-cmd uid uid metadata-id new-metadata))
  ([uid ugroup metadata-id new-metadata]
   (c/send-command!
    @comms
    :kixi.datastore.metadatastore/update
    "1.0.0"
    {:kixi.user/id uid
     :kixi.user/groups (vec-if-not ugroup)}
    (assoc new-metadata
           ::ms/id metadata-id)
    {:kixi.comms.command/partition-key metadata-id})))

(defn send-update-event
  [uid ugroup event]
  (c/send-event!
   @comms
   :kixi.datastore.file-metadata/updated
   "1.0.0"
   event
   {:kixi.comms.event/partition-key (get-in event [::ms/file-metadata ::ms/id])}))

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
    spec
    {:kixi.comms.command/partition-key uid})))

(defn send-multi-part-upload-link-cmd
  ([uid size-bytes]
   (send-multi-part-upload-link-cmd uid uid size-bytes))
  ([uid ugroup size-bytes]
   (let [id (uuid)]
     (c/send-valid-command!
      @comms
      {::cmd/type :kixi.datastore.filestore/initiate-file-upload
       ::cmd/version "1.0.0"
       :kixi/user {:kixi.user/id uid
                   :kixi.user/groups (vec-if-not ugroup)}
       ::cmd/id id
       ::fsupload/size-bytes size-bytes}
      {:partition-key id}))))

(defn send-complete-multi-part-upload-cmd
  ([uid etags file-id]
   (send-complete-multi-part-upload-cmd uid uid etags file-id))
  ([uid ugroup etags file-id]
   (c/send-valid-command!
    @comms
    {::cmd/type :kixi.datastore.filestore/complete-file-upload
     ::cmd/version "1.0.0"
     :kixi/user {:kixi.user/id uid
                 :kixi.user/groups (vec-if-not ugroup)}
     :kixi.datastore.metadatastore/id file-id
     ::fsupload/part-ids etags
     ::fs/id file-id}
    {:partition-key file-id})))

(defn send-malformed-multi-part-upload-cmd
  "This command misses the required `size-bytes` filed which should trigger a failed event"
  ([uid]
   (send-malformed-multi-part-upload-cmd uid uid))
  ([uid ugroup]
   (c/send-valid-command!
    @comms
    {::cmd/type :kixi.datastore.filestore/initiate-file-upload
     ::cmd/version "1.0.0"
     :kixi/user {:kixi.user/id uid
                 :kixi.user/groups (vec-if-not ugroup)}}
    {:partition-key uid})))

(defn event-for
  [uid event]
  (= uid
     (or (get-in event [:kixi/user :kixi.user/id])
                                        ;old formats
         (get-in event [:kixi.comms.event/payload :schema ::ss/provenance :kixi.user/id])
         (get-in event [:kixi.comms.event/payload ::ss/provenance :kixi.user/id])
         (get-in event [:kixi.comms.event/payload ::ms/file-metadata ::ms/provenance :kixi.user/id])
         (get-in event [:kixi.comms.event/payload :kixi.user/id])
         (get-in event [:kixi.comms.event/payload :kixi/user :kixi.user/id]))))

(defn wait-for-events
  [uid & event-types]
  (first
   (async/alts!!
    (mapv (fn [c]
            (async/go-loop
                [event (async/<! c)]
              (if (and (event-for uid event)
                       ((set event-types)
                        (or (:kixi.comms.event/key event)
                            (::event/type event))))
                event
                (when event
                  (recur (async/<! c))))))
          [@event-channel
           (async/timeout (* wait-tries
                             wait-per-try))]))))

(defn send-spec
  [uid spec]
  (send-spec-no-wait uid spec)
  (let [event (wait-for-events uid :kixi.datastore.schema/rejected :kixi.datastore.schema/created)]
    (if (= :kixi.datastore.schema/created
           (:kixi.comms.event/key  event))
      (wait-for-url uid (schema-url
                         (get-in event [:kixi.comms.event/payload ::ss/id])))
      event)))

(defn metadata->user-id
  [metadata]
  (get-in metadata [::ms/provenance :kixi.user/id]))

(defn file-redirect-by-id
  ([uid id]
   (file-redirect-by-id uid uid id))
  ([uid user-groups id]
   (let [url (file-download-url id)]
     (try (client/get url {:headers {"user-id" uid
                                     "user-groups" (vec-if-not user-groups)}
                           :follow-redirects false
                           :redirect-strategy :none
                           :throw-exceptions false})
          (catch org.apache.http.ProtocolException e
            (if (clojure.string/starts-with? (.getMessage e) "Redirect URI does not specify a valid host name: file:///")
              {:status 302
               :headers {"Location" (subs (.getMessage e)
                                          (clojure.string/index-of
                                           (.getMessage e)
                                           "file://"))}}
              (throw e)))))))

(defn get-upload-link-event
  [user-id]
  (send-upload-link-cmd user-id)
  (wait-for-events user-id :kixi.datastore.filestore/upload-link-created))

(defn get-multi-part-upload-links-event
  [user-id size-bytes]
  (send-multi-part-upload-link-cmd user-id size-bytes)
  (wait-for-events user-id :kixi.datastore.filestore/file-upload-initiated))

(defn get-upload-link
  [user-id]
  (let [link-event (get-upload-link-event user-id)]
    [(get-in link-event [:kixi.comms.event/payload :kixi.datastore.filestore/upload-link])
     (get-in link-event [:kixi.comms.event/payload :kixi.datastore.filestore/id])]))

(defn get-multi-part-upload-links
  [user-id size-bytes]
  (let [multi-part-links-event (get-multi-part-upload-links-event user-id size-bytes)]
    multi-part-links-event))

(defn get-dload-link-event
  [user-id user-groups id]
  (file-redirect-by-id user-id user-groups id)
  (wait-for-events user-id
                   :kixi.datastore.filestore/download-link-created
                   :kixi.datastore.filestore/download-link-rejected))

(defn get-dload-link
  ([user-id id]
   (get-dload-link
    user-id user-id id))
  ([user-id user-groups id]
   (let [link-event (get-dload-link-event user-id user-groups id)]
     (get-in link-event [:kixi.comms.event/payload ::fs/link]))))

(defn strip-protocol
  [^String path]
  (subs path
        (+ 3 (.indexOf path
                       ":"))))

(defmulti upload-file
  (fn [^String target file-name]
    (subs target 0
          (.indexOf target
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

(defmulti upload-multi-part-file-request
  (fn [^String target file-name]
    (subs target 0
          (.indexOf target
                    ":"))))

(defmethod upload-multi-part-file-request "file"
  [target buffer]
  (io/copy buffer
           (doto (io/file (strip-protocol target))
             (.createNewFile)))
  ;; return a fake etag
  (uuid))

(defmethod upload-multi-part-file-request "https"
  [target buffer]
  (get-in
   (client/put target {:body buffer})
   [:headers "ETag"]))

(defn upload-multi-part-file
  [links uid file-name file-id]
  (let [file (io/file file-name)
        upload-part (fn [^java.io.InputStream reader
                         {:keys [::fsupload/url
                                 ::fsupload/start-byte
                                 ::fsupload/length-bytes]}]
                      (let [buffer (byte-array length-bytes)]
                        (log/info "...uploading" start-byte ":" length-bytes)
                        (.read reader buffer 0 length-bytes)
                        (upload-multi-part-file-request url buffer)))
        send-complete (fn [etags]
                        (send-complete-multi-part-upload-cmd uid etags file-id)
                        (let [e (wait-for-events uid :kixi.datastore.filestore/file-upload-completed
                                                 :kixi.datastore.filestore/file-upload-rejected)]
                          (is (= :kixi.datastore.filestore/file-upload-completed
                                 (:kixi.event/type e)))))]
    (log/info "Uploading file" file-name file-id "in" (count links) "parts." )
    (with-open [r (io/input-stream file)]
      (->> links
           (map (partial upload-part r))
           doall
           send-complete))))

(defn create-metadata
  ([uid file-name]
   (create-metadata uid file-name nil))
  ([uid file-name schema-id]
   (create-metadata
    {:file-name file-name
     :type "stored"
     :sharing {::ms/file-read [uid]
               ::ms/meta-read [uid]
               ::ms/meta-update [uid]}
     :provenance {::ms/source "upload"
                  :kixi.user/id uid}
     :size-bytes (file-size file-name)
     :schema-id schema-id
     :header true}))
  ([{:keys [^String file-name type schema-id user-groups sharing header size-bytes provenance]}]
   (merge {}
          (when type
            {::ms/type type})
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

(defn send-multi-part-file-and-metadata-no-wait
  ([metadata]
   (send-multi-part-file-and-metadata-no-wait
    (metadata->user-id metadata)
    (metadata->user-id metadata)
    metadata))
  ([uid ugroup metadata]
   (let [links (get-multi-part-upload-links ugroup (::ms/size-bytes metadata))
         {:keys [:kixi.datastore.filestore/id
                 :kixi.datastore.filestore.upload/part-urls]} links]
     (is id)
     (is (not-empty part-urls))
     (let [md-with-id (assoc metadata ::ms/id id)]
       (when (not-empty part-urls)
         (upload-multi-part-file part-urls
                                 uid
                                 (::ms/name md-with-id)
                                 id)
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
   (send-file-and-metadata-no-wait uid ugroup metadata)
   (let [event (wait-for-events uid
                                :kixi.datastore.file-metadata/rejected
                                :kixi.datastore.file-metadata/updated)]
     (if (= :kixi.datastore.file-metadata/updated
            (:kixi.comms.event/key event))
       (wait-for-metadata-to-be-searchable ugroup
                                           (get-in event [:kixi.comms.event/payload
                                                          ::ms/file-metadata
                                                          ::ms/id]))
       event))))

(defn send-multi-part-file-and-metadata
  ([metadata]
   (send-multi-part-file-and-metadata
    (metadata->user-id metadata)
    (metadata->user-id metadata)
    metadata))
  ([uid ugroup metadata]
   (send-multi-part-file-and-metadata-no-wait uid ugroup metadata)
   (let [event (wait-for-events uid :kixi.datastore.file-metadata/rejected :kixi.datastore.file-metadata/updated)]
     (if (= :kixi.datastore.file-metadata/updated
            (:kixi.comms.event/key event))
       (wait-for-metadata-to-be-searchable ugroup
                                           (get-in event [:kixi.comms.event/payload
                                                          ::ms/file-metadata
                                                          ::ms/id]))
       event))))


(defn create-datapack
  ([uid ugroup pack-name bundled-ids]
   (create-datapack
    {:type "bundle"
     :bundle-type "datapack"
     :pack-name pack-name
     :sharing {::ms/file-read (vec-if-not ugroup)
               ::ms/meta-read (vec-if-not ugroup)
               ::ms/meta-update (vec-if-not ugroup)
               ::ms/bundle-add (vec-if-not ugroup)}
     :bundled-ids bundled-ids
     :provenance {::ms/source "upload"
                  :kixi.user/id uid}}))
  ([{:keys [^String pack-name bundled-ids sharing header provenance id type bundle-type]}]
   (merge {::ms/id (or id (uuid))
           ::ms/type type
           ::ms/bundle-type bundle-type
           ::ms/name pack-name
           ::ms/bundled-ids bundled-ids}
          (when sharing
            {::ms/sharing sharing})
          (when provenance
            {::ms/provenance provenance}))))

(defn send-datapack
  ([uid pack-name bundled-ids]
   (send-datapack uid uid pack-name bundled-ids))
  ([uid ugroup pack-name bundled-ids]
   (send-datapack (create-datapack uid ugroup pack-name bundled-ids)))
  ([metadata]
   (let [ugroup (or (get-in metadata [::ms/sharing ::ms/file-read])
                    (get-in metadata [::ms/sharing ::ms/meta-read])
                    (get-in metadata [::ms/sharing ::ms/meta-update]))
         uid (get-in metadata [::ms/provenance :kixi.user/id])]
     (send-datapack-cmd ugroup
                        metadata)
     (let [event (wait-for-events uid :kixi.datastore.file-metadata/rejected :kixi.datastore.file-metadata/updated)]
       (if (= :kixi.datastore.file-metadata/updated
              (:kixi.comms.event/key event))
         (wait-for-metadata-key ugroup
                                (get-in event [:kixi.comms.event/payload
                                               ::ms/file-metadata
                                               ::ms/id])
                                ::ms/id)
         event)))))

(defn send-file-delete
  ([uid meta-id]
   (send-file-delete uid uid meta-id))
  ([uid ugroups meta-id]
   (send-file-delete-cmd uid ugroups meta-id)
   (wait-for-events uid :kixi.datastore/file-deleted :kixi.datastore/file-delete-rejected)))

(defn send-bundle-delete
  ([uid meta-id]
   (send-bundle-delete uid uid meta-id))
  ([uid ugroups meta-id]
   (send-bundle-delete-cmd uid ugroups meta-id)
   (wait-for-events uid :kixi.datastore/bundle-deleted :kixi.datastore/bundle-delete-rejected)))

(defn send-add-files-to-bundle
  ([uid id bundled-ids]
   (send-add-files-to-bundle uid uid id bundled-ids))
  ([uid ugroups id bundled-ids]
   (send-add-files-to-bundle-cmd uid ugroups id bundled-ids)
   (let [event (wait-for-events uid
                                :kixi.datastore/files-added-to-bundle
                                :kixi.datastore/files-add-to-bundle-rejected)]
     (when (= :kixi.datastore/files-added-to-bundle
              (or (:kixi.comms.event/key event)
                  (::event/type event)))
       (wait-for-pred #(let [metadata (:body (get-metadata ugroups id))
                             bundled-ids (set (::ms/bundled-ids metadata))]
                         (clojure.set/subset? (set (vec-if-not bundled-ids)) bundled-ids))))
     event)))

(defn send-remove-files-from-bundle
  ([uid id bundled-ids]
   (send-remove-files-from-bundle uid uid id bundled-ids))
  ([uid ugroups id bundled-ids]
   (send-remove-files-from-bundle-cmd uid ugroups id bundled-ids)
   (let [event (wait-for-events uid
                                :kixi.datastore/files-removed-from-bundle
                                :kixi.datastore/files-remove-from-bundle-rejected)]
     (when (= :kixi.datastore/files-removed-from-bundle
              (or (:kixi.comms.event/key event)
                  (::event/type event)))
       (wait-for-pred #(let [metadata (:body (get-metadata ugroups id))
                             bundled-ids (set (::ms/bundled-ids metadata))]
                         (empty? (clojure.set/intersection bundled-ids (set (vec-if-not bundled-ids)))))))
     event)))

(defn update-metadata-sharing-old
  ([uid metadata-id change-type activity target-group]
   (update-metadata-sharing-old uid uid metadata-id change-type activity target-group))
  ([uid ugroup metadata-id change-type activity target-group]
   (send-metadata-sharing-change-cmd-old uid ugroup metadata-id change-type activity target-group)
   (let [event (wait-for-events uid
                                :kixi.datastore.metadatastore/sharing-change-rejected
                                :kixi.datastore.file-metadata/updated)]
     (when (= :kixi.datastore.file-metadata/updated
              (or (:kixi.comms.event/key event)
                  (::event/type event)))
       (wait-for-pred #(let [metadata (:body (get-metadata ugroup metadata-id))
                             activity-set (set (get (::ms/sharing metadata) activity))]
                         (case change-type
                           ::ms/sharing-conj (contains? activity-set target-group)
                           ::ms/sharing-disj ((complement contains?) activity-set target-group)))))
     event)))

(defn update-metadata-sharing
  ([uid metadata-id change-type activity target-group]
   (update-metadata-sharing uid uid metadata-id change-type activity target-group))
  ([uid ugroup metadata-id change-type activity target-group]
   (send-metadata-sharing-change-cmd uid ugroup metadata-id change-type activity target-group)
   (let [event (wait-for-events uid
                                :kixi.datastore/sharing-change-rejected
                                :kixi.datastore/sharing-changed)]
     (when (= :kixi.datastore/sharing-changed
              (or (:kixi.comms.event/key event)
                  (::event/type event)))
       (wait-for-pred #(let [metadata (:body (get-metadata ugroup metadata-id))
                             activity-set (set (get (::ms/sharing metadata) activity))]
                         (case change-type
                           ::ms/sharing-conj (contains? activity-set target-group)
                           ::ms/sharing-disj ((complement contains?) activity-set target-group)))))
     event)))

(defn update-metadata
  ([uid metadata-id new-metadata]
   (update-metadata uid uid metadata-id new-metadata))
  ([uid ugroup metadata-id new-metadata]
   (send-metadata-update-cmd uid ugroup metadata-id new-metadata)
   (wait-for-events uid :kixi.datastore.metadatastore/update-rejected :kixi.datastore.file-metadata/updated)))

(defn post-segmentation
  [url seg]
  (client/post url
               {:form-params seg
                :headers {"user-id" (uuid)
                          "user-groups" (uuid)}
                :content-type :json
                :throw-exceptions false
                :as :json}))

(defn get-spec
  [ugroup id]
  (client/get (schema-url id)
              {:accept :transit+json
               :as :transit+json
               :headers {"user-id" (uuid)
                         "user-groups" ugroup}
               :throw-exceptions false}))

(defn extract-schema
  [r-g]
  (when (= 200 (:status r-g))
    (:body r-g)))

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
                                              :headers {"user-id" uid
                                                        "user-groups" uid}}))
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
      (if r
        (throw (new Exception (str "Recieved non-200 response trying to fetch schema:" {:resp r})))
        (throw (new Exception "No acceptance or rejection response event seen for schema command"))))))

(defmacro has-status
  [status resp]
  `(is-submap {:status ~status}
              ~resp))

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

(defmacro when-event-key
  [event k & rest]
  `(let [k-val# (or (:kixi.comms.event/key ~event)
                    (:kixi.event/type ~event))]
     (is (= ~k
            k-val#))
     (when (= ~k
              k-val#)
       ~@rest)))

(defmacro when-event-type
  [event k & rest]
  `(let [k-val# (:kixi.event/type ~event)]
     (is (= ~k
            k-val#))
     (when (= ~k
              k-val#)
       ~@rest)))


(defn is-file-metadata-rejected
  [uid deliverer rejection-submap]
  (deliverer)
  (let [e (wait-for-events uid :kixi.datastore.file-metadata/rejected :kixi.datastore.file-metadata/update)]
    (is e
        "Rejection message not received")
    (when e
      (is-submap rejection-submap
                 (:kixi.comms.event/payload e)))))

(defn small-file-into-datapack
  ([uid]
   (small-file-into-datapack uid {}))
  ([uid extra-dp-meta]
   (let [metadata-response (send-file-and-metadata
                            (create-metadata
                             uid
                             "./test-resources/metadata-one-valid.csv"))]
     (when-success metadata-response
       (let [datapack-resp (send-datapack (merge (create-datapack uid uid "small-file-into-a-datapack" #{(extract-id metadata-response)})
                                                 extra-dp-meta))]
         datapack-resp)))))
