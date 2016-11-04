(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [clojure.java.io :as io]
            [clojure.spec :as spec]
            [cheshire.core :as json]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as ds]
            [kixi.datastore.metadatastore :as ms]
            [kixi.comms :as c]
            [kixi.datastore.communication-specs :as cs]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.validator :as sv]
            [kixi.datastore.segmentation :as seg]
            [kixi.datastore.sharing :as share]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [yada
             [resource :as yr]
             [yada :as yada]]
            [yada.resources.webjar-resource :refer [new-webjar-resource]]
            [kixi.datastore.transit :as t]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.transport-specs :as ts]))

(defn ctx->user-id
  [ctx]
  (get-in ctx [:request :headers "user-id"]))

(defn vec-if-not
  [x]
  (if (or (nil? x)
          (vector? x))
    x
    (vector x)))

(defn ctx->user-groups
  [ctx]
  (vec-if-not (get-in ctx [:request :headers "user-groups"])))

(defn say-hello [ctx]
  (info "Saying hello")
  (str "Hello " (get-in ctx [:parameters :query :p]) "!\n"))

(defn yada-timbre-logger
  [ctx]
  (when (= 500 (get-in ctx [:response :status]))
    (if (:error ctx)
      (error (:error ctx) "Server error")
      (error "Server error, no exception available")))
  ctx)

(defn append-error-interceptor
  [res point & interceptors]
  (update res :error-interceptor-chain
          (partial mapcat (fn [i]
                            (if (= i point)
                              (concat [i] interceptors)
                              [i])))))

(spec/def ::context
  (spec/keys :req []
             :opts []))

(spec/def ::error #{:schema-invalid-request
                    :unknown-schema :file-upload-failed})
(spec/def ::msg (spec/keys :req []
                           :opts []))
(spec/def ::error-map
  (spec/keys :req [::error ::msg]))

(spec/fdef return-error
        :args (spec/cat :ctx ::context
                        :args (spec/alt :error-map ::error-map
                                        :error-parts (spec/cat :error ::error
                                                               :msg ::msg)
                                        :spec-error ::ms/explain)))

(defn return-error
  ([ctx error]
   (assoc (:response ctx)
          :status 400
          :body error))
  ([ctx error-key msg]
   (return-error ctx {::error error-key
                      ::msg msg})))

(defn return-unauthorised
  [ctx]
  (assoc (:response ctx)
         :status 401))

(defn resource
  [metrics model]
  (-> model
      (assoc :logger yada-timbre-logger)
      (assoc :responses {500 {:produces "text/plain"
                              :response (fn [ctx] "Server Error, see logs")}})
      yada/resource
      (yr/insert-interceptor
       yada.interceptors/available? (:insert-time-in-ctx metrics))
      (yr/append-interceptor
       yada.interceptors/create-response (:record-ctx-metrics metrics))
      (append-error-interceptor
       yada.interceptors/create-response (:record-ctx-metrics metrics))))

(defn hello-parameters-resource
  [metrics]
  (resource
   metrics
   {:methods
    {:get
     {:parameters {:query {:p s/Str}}
      :produces "text/plain"
      :response say-hello}}}))

(defn hello-routes
  [metrics]
  ["" [["/hello" (yada/handler "Hello World!\n")]
       ["/hello-param" (hello-parameters-resource metrics)]]])

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn add-part
  [part state]
  (update state :parts (fnil conj [])
          (yada.multipart/map->DefaultPart part)))

(defn payload-size
  [{:keys [bytes body-offset] :as piece-or-part}]
  (- (alength ^bytes bytes)
     (or body-offset 0)))

(defn transfer-piece!
  [^java.io.OutputStream output-stream piece & [close]]
  (try
    (let [body-offset (or (:body-offset piece) 0)]
      (.write output-stream
              ^bytes (:bytes piece)
              body-offset
              (- (alength ^bytes (:bytes piece))
                 body-offset)))
    (.flush output-stream)
    (finally
      (when (or close false)
        (.close output-stream)))))

(defrecord StreamingPartial [id output-stream initial size-bytes]
  yada.multipart/Partial
  (continue [this piece]
    (transfer-piece! output-stream piece (:body-offset piece))
    (-> this
        (update :pieces-count (fnil inc 0))
        (update :size-bytes (partial + (payload-size piece)))))
  (complete [this state piece]
    (transfer-piece! output-stream piece true)
    (-> initial
        (assoc :type :part
               :count (:pieces-count this)
               :id id
               :size-bytes (+ size-bytes
                              (payload-size piece)))
        (add-part state))))

(defn flush-tiny-file!
  "Files smaller than the buffer size (?) come through as complete parts"
  [filestore id part]
  (transfer-piece! (ds/output-stream filestore id)
                   part
                   true))

(defn file-part?
  [part]
  (= "application/octet-stream"
     (get-in part [:headers "content-type"])))

(def Map {s/Keyword s/Any})

(defrecord PartConsumer
    [filestore]
    yada.multipart/PartConsumer
    (consume-part [_ state part]
      (if (file-part? part)
        (let [id (uuid)]
          (flush-tiny-file! filestore id part)
          (-> part
              (assoc :id id
                     :count 1
                     :size-bytes (payload-size part))
              (dissoc :bytes)
              (add-part state)))
        (add-part part state)))
    (start-partial [_ piece]
      (let [id (uuid)
            out (ds/output-stream filestore id)]
        (transfer-piece! out piece)
        (->StreamingPartial id out
                            (dissoc piece :bytes)
                            (payload-size piece))))
    (part-coercion-matcher [s]
      "Return a map between a target type and the function that coerces this type into that type"
      {s/Str (fn [part]
               (let [offset (or (:body-offset part) 0)]
                 (String. ^bytes (:bytes part)
                          ^int offset
                          ^int (- (alength ^bytes (:bytes part)) offset))))}))

(defn file-create
  [metrics filestore communications schemastore]
  (resource
   metrics
   {:id :file-create
    :methods
    {:post
     {:consumes "multipart/form-data"
      :produces "application/json"
      :parameters {:body {:file Map
                          :file-metadata s/Str}}
      :part-consumer (map->PartConsumer {:filestore filestore})
      :response (fn [ctx]
                  (let [params (get-in ctx [:parameters :body])
                        file (:file params)
                        declared-file-size (:size-bytes file) ;obviously broken, want to come from header                        
                        transported-metadata (json/parse-string (:file-metadata params) keyword)
                        file-details {::ms/id (:id file)
                                      ::ms/size-bytes (:size-bytes file)
                                      ::ms/provenance {::ms/source "upload"
                                                       ::ms/pieces-count (:count file)
                                                       :kixi.user/id (ctx->user-id ctx)}}
                        metadata (ts/filemetadata-transport->internal
                                  (dissoc transported-metadata :user-id)
                                  file-details)]
                    (let [error (or (spec/explain-data ::ms/file-metadata metadata)
                                    (when-not (ss/exists schemastore (::ss/id metadata))
                                      {::error :unknown-schema
                                       ::msg {:schema-id (::ss/id metadata)}})
                                    (when-not (= declared-file-size
                                                 (:size-bytes file))
                                      {::error :file-upload-failed
                                       ::msg {:declared-size declared-file-size
                                              :recieved-size (:size-bytes file)}}))]
                      (if error
                        (return-error ctx error)
                        (do
                          (let [conformed (spec/conform ::ms/file-metadata metadata)]
                            (cs/send-event! communications
                                            (assoc conformed
                                                   ::cs/event :kixi.datastore/file-created
                                                   ::cs/version "1.0.0"))
                            (cs/send-event! communications {::cs/event :kixi.datastore/file-metadata-updated
                                                            ::cs/version "1.0.0"
                                                            ::cs/file-metadata-update-type 
                                                            ::cs/file-metadata-created
                                                            ::ms/file-metadata conformed}))
                          (java.net.URI. (:uri (yada/uri-for ctx :file-entry {:route-params {:id (::ms/id metadata)}}))))))))}}}))

(defn file-entry
  [metrics filestore sharing]
  (resource
   metrics
   {:id :file-entry
    :methods
    {:get {:produces [{:media-type #{"application/octet-stream"}}]
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (if (share/authorised sharing ::ms/sharing :file-read id (ctx->user-groups ctx))
                 (ds/retrieve filestore
                              id)
                 (return-unauthorised ctx))))}}}))

(defn file-meta
  [metrics metadatastore sharing]
  (resource
   metrics
   {:id :file-meta
    :methods
    {:get {:produces "application/json"
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (if (share/authorised sharing ::ms/sharing :meta-read id (ctx->user-groups ctx))
                 (ms/fetch metadatastore id)
                 (return-unauthorised ctx))))}}}))

(defn file-segmentation-create
  [metrics communications metadatastore]
  (resource
   metrics
   {:id :file-segmentation
    :methods
    {:post {:consumes "application/json"
            :response
            (fn [ctx]
              (let [id (uuid)
                    file-id (get-in ctx [:parameters :path :id])
                    body (get-in ctx [:body])
                    type (:type body)
                    user-id (ctx->user-id ctx)]
                (if (ms/exists metadatastore file-id)
                  (do
                    (case type
                      "column" (let [col-name (:column-name body)]
                                 (cs/send-event! communications 
                                                 {::cs/event :kixi.datastore/file-segmentation-created
                                                  ::cs/version"1.0.0"
                                                  :kixi.datastore.request/type ::seg/group-rows-by-column
                                                  ::seg/id id
                                                  ::ms/id file-id
                                                  ::seg/column-name col-name
                                                  :kixi.user/id user-id})))
                    (java.net.URI. (:uri (yada/uri-for ctx :file-segmentation-entry {:route-params {:segmentation-id id
                                                                                                    :id file-id}}))))
                  (assoc (:response ctx) ;don't know why i'm having to do this here...
                         :status 404))))}}}))

(defn file-segmentation-entry
  [metrics filestore]
  (resource
   metrics
   {:id :file-segmentation-entry
    :methods
    {:get {:produces "application/json"
           :response
           (fn [ctx]
                                        ;get all the segmentation information (each segment is a FILE!)
             (let [id (get-in ctx [:parameters :path :id])]
               (ds/retrieve filestore
                            id)))}}}))

(defn schema-id-entry
  [metrics schemastore]
  (resource
   metrics
   {:id :schema-id-entry
    :methods
    {:get {:produces "application/json"
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (ss/fetch-with schemastore {::ss/id id})))}}}))

(defn schema-resources
  [metrics schemastore communications]
  (resource
   metrics
   {:id :schema-create
    :methods
    {:post {:consumes "application/json"
            :produces "application/json"
            :response (fn [ctx]
                        (let [new-id      (uuid)
                              body        (get-in ctx [:body])
                              raw-schema-req (assoc body
                                                    :id new-id)
                              conformed-sr (ts/keywordize-values
                                            (ts/add-ns-to-keys ::ss/_ 
                                                               raw-schema-req))]
                          (if-not (spec/valid?
                               ::ts/schema-transport
                               conformed-sr)
                            (return-error ctx :schema-invalid-request
                                          (spec/explain-data ::ts/schema-transport
                                                             conformed-sr))
                            (let [internal-sr (ts/schema-transport->internal 
                                               (ts/raise-spec conformed-sr))]
                              (if-let [preexists (ss/fetch-with schemastore
                                                                (select-keys internal-sr
                                                                             [::ss/name
                                                                              ::ss/schema]))]
                                (assoc (:response ctx)
                                       :status 202 ;; wants to be a 303
                                       :headers {"Location"
                                                 (str (java.net.URI.
                                                       (:uri (yada/uri-for
                                                              ctx
                                                              :schema-id-entry
                                                              {:route-params {:id (::ss/id preexists)}}))))})
                                (do
                                  (cs/send-event! communications 
                                                  (merge {::cs/event :kixi.datastore/schema-created
                                                          ::cs/version "1.0.0"}
                                                         internal-sr))
                                  (assoc (:response ctx)
                                         :status 202
                                         :headers {"Location"
                                                   (java.net.URI.
                                                    (:uri (yada/uri-for
                                                           ctx
                                                           :schema-id-entry
                                                           {:route-params {:id new-id}})))})))))))}}}))

(defn healthcheck
  [ctx]
                                        ;Return truthy for now, but later check dependancies
  "All is well")

(defn service-routes
  [metrics filestore metadatastore communications schemastore sharing]
  [""
   [["/file" [["" (file-create metrics filestore communications schemastore)]
              [["/" :id] (file-entry metrics filestore sharing)]
              [["/" :id "/meta"] (file-meta metrics metadatastore sharing)]
              [["/" :id "/segmentation"] (file-segmentation-create metrics communications metadatastore)]
              [["/" :id "/segmentation/" :segmentation-id] (file-segmentation-entry metrics communications)]
                                        ;              [["/" :id "/segment/" :segment-type "/" :segment-value] (file-segment-entry metrics filestore)]
              ]]
    ["/schema" [[["/"]     (schema-resources metrics schemastore communications)]
                [["/" :id] (schema-id-entry metrics schemastore)]]]]])

(defn routes
  "Create the URI route structure for our application."
  [metrics filestore metadatastore communications schemastore sharing]
  [""
   [(hello-routes metrics)
    (service-routes metrics filestore metadatastore communications schemastore sharing)
    ["/healthcheck" healthcheck]

    #_      ["/api" (-> roots
                        ;; Wrap this route structure in a Swagger
                        ;; wrapper. This introspects the data model and
                        ;; provides a swagger.json file, used by Swagger UI
                        ;; and other tools.
                        (yada/swaggered
                         {:info {:title "Kixi Datastore"
                                 :version "1.0"
                                 :description "Testing api resource UI"}
                          :basePath "/api"})
                        ;; Tag it so we can create an href to this API
                        (tag :edge.resources/api))]

    ["/metrics" (yada/resource (:expose-metrics-resource metrics))]

    ;; Swagger UI
    ["/swagger" (-> (new-webjar-resource "/swagger-ui" {})
                    ;; Tag it so we can create an href to the Swagger UI
                    (tag :edge.resources/swagger))]

    ;; This is a backstop. Always produce a 404 if we ge there. This
    ;; ensures we never pass nil back to Aleph.
    [true (yada/handler nil)]]])

(defrecord WebServer
    [port listener log-config metrics filestore metadatastore communications schemastore sharing]
    component/Lifecycle
    (start [component]
      (if listener
        component                       ; idempotence
        (let [vhosts-model
              (vhosts-model
               [{:scheme :http :host (format "localhost:%d" port)}
                (routes metrics filestore metadatastore communications schemastore sharing)])
              listener (yada/listener vhosts-model {:port port})]
          (infof "Started web-server on port %s" port)
          (assoc component :listener listener))))
    (stop [component]
      (when-let [close (get-in component [:listener :close])]
        (infof "Stopping web-server on port %s" port)
        (close))
      (assoc component :listener nil)))

(defn new-web-server [config]
  (map->WebServer (:web-server config)))
