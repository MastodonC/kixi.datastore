(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [cheshire.core :as json]
            [clojure.spec :as spec]
            [clojure.core.async :as async :refer [<!!]]
            [com.stuartsierra.component :as component]
            [kixi.datastore
             [communication-specs :as cs]
             [filestore :as ds]
             [metadatastore :as ms]
             [schemastore :as ss]
             [segmentation :as seg]
             [transport-specs :as ts]]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [yada
             [resource :as yr]
             [yada :as yada]]
            [yada.resources.webjar-resource :refer [new-webjar-resource]]))

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

(defn part->string
  [part]
  (let [offset (or (:body-offset part) 0)]
    (String. ^bytes (:bytes part)
             ^int offset
             ^int (- (alength ^bytes (:bytes part)) offset))))

(defn part->long
  [part]
  (let [offset (or (:body-offset part) 0)]
    (Long/valueOf
     (String. ^bytes (:bytes part)
              ^int offset
              ^int (- (alength ^bytes (:bytes part)) offset)))))

(defn file-part?
  [part]
  (= "application/octet-stream"
     (get-in part [:headers "content-type"])))

(defn file-size-part?
  [part]
  (= "file-size"
     (get-in part [:content-disposition :params "name"])))

(defn last-file-size
  [state]
  (last (:sizes state)))

(defn add-part
  [part state]
  (let [added (update state :parts (fnil conj [])
                      (yada.multipart/map->DefaultPart part))]
    (if (file-size-part? part)
      (let [^Long size (part->long part)]
        (update added :sizes (fnil conj [])
                size))
      added)))

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

(defrecord StreamingPartial [id ^java.io.OutputStream output-stream complete-chan initial size-bytes]
  yada.multipart/Partial
  (continue [this piece]
    (transfer-piece! output-stream piece (:body-offset piece))
    (-> this
        (update :pieces-count (fnil inc 0))
        (update :size-bytes (partial + (payload-size piece)))))
  (complete [this state piece]
    (transfer-piece! output-stream piece true)
    (<!! complete-chan)
    (-> initial
        (assoc :type :part
               :count (:pieces-count this)
               :id id
               :size-bytes (+ size-bytes
                              (payload-size piece)))
        (add-part state))))

(defn flush-tiny-file!
  "Files smaller than the buffer size (?) come through as complete parts"
  [filestore id part file-size]
  (let [[complete-chan out] (ds/output-stream filestore id file-size)]
    (transfer-piece! out
                     part
                     true)
    (<!! complete-chan)))

(def Map {s/Keyword s/Any})

(defrecord PartConsumer
    [filestore]
    yada.multipart/PartConsumer
    (consume-part [this state part]
      (if (file-part? part)
        (let [id (uuid)]
          (flush-tiny-file! filestore id part (last-file-size state))
          (-> part
              (assoc :id id
                     :count 1
                     :size-bytes (payload-size part))
              (dissoc :bytes)
              (add-part state)))
        (add-part part state)))
    (start-partial [this state piece]
      (let [id (uuid)
            [complete-chan out] (ds/output-stream filestore id (last-file-size state))]
        (transfer-piece! out piece)
        (->StreamingPartial id out complete-chan
                            (dissoc piece :bytes)
                            (payload-size piece)))))

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
                          :file-metadata s/Str
                          :file-size s/Str}}
      :part-consumer (map->PartConsumer {:filestore filestore})
      :response (fn [ctx]
                  (let [params (get-in ctx [:parameters :body])
                        file (:file params)
                        declared-file-size (Long/valueOf (str (:file-size params)))
                        transported-metadata (json/parse-string (:file-metadata params) keyword)
                        file-details {::ms/id (:id file)
                                      ::ms/size-bytes (:size-bytes file)
                                      ::ms/provenance {::ms/source "upload"
                                                       ::ms/pieces-count (:count file)
                                                       :kixi.user/id (ctx->user-id ctx)}}
                        metadata (ts/filemetadata-transport->internal
                                  (dissoc transported-metadata :user-id)
                                  file-details)
                        explained (spec/explain-data ::ms/file-metadata metadata)]
                    (cond
                      explained (return-error ctx explained)
                      (not (ss/exists schemastore (::ss/id metadata))) (return-error ctx 
                                                                                  {::error :unknown-schema
                                                                                   ::msg {:schema-id (::ss/id metadata)}})
                      (not (ss/authorised schemastore
                                          ::ss/use
                                          (::ss/id metadata)
                                          (ctx->user-groups ctx))) (return-unauthorised ctx)
                      (not= declared-file-size (:size-bytes file)) (return-error ctx                                               
                                                                              {::error :file-upload-failed
                                                                               ::msg {:declared-size declared-file-size
                                                                                      :recieved-size (:size-bytes file)}})
                      :else (let [conformed (spec/conform ::ms/file-metadata metadata)]
                              (cs/send-event! communications
                                              (assoc conformed
                                                     ::cs/event :kixi.datastore/file-created
                                                     ::cs/version "1.0.0"))
                              (cs/send-event! communications {::cs/event :kixi.datastore/file-metadata-updated
                                                              ::cs/version "1.0.0"
                                                              ::cs/file-metadata-update-type 
                                                              ::cs/file-metadata-created
                                                              ::ms/file-metadata conformed})
                              (java.net.URI.
                               (yada/url-for ctx :file-entry {:route-params {:id (::ms/id metadata)}}))))))}}}))

(defn file-entry
  [metrics filestore metadatastore]
  (resource
   metrics
   {:id :file-entry
    :methods
    {:get {:produces [{:media-type #{"application/octet-stream"}}]
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (if (ms/authorised metadatastore :file-read id (ctx->user-groups ctx))
                 (ds/retrieve filestore
                              id)
                 (return-unauthorised ctx))))}}}))

(defn file-meta
  [metrics metadatastore]
  (resource
   metrics
   {:id :file-meta
    :methods
    {:get {:produces "application/json"
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (if (ms/authorised metadatastore :meta-read id (ctx->user-groups ctx))
                 (ms/retrieve metadatastore id)
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
                    (java.net.URI.
                     (yada/url-for ctx :file-segmentation-entry {:route-params {:segmentation-id id
                                                                                :id file-id}})))
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
               (when (ss/exists schemastore id)
                 (if (ss/authorised schemastore ::ss/read id (ctx->user-groups ctx))
                   (ss/retrieve schemastore id)
                   (return-unauthorised ctx)))))}}}))

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
                              internal-sr (ts/schema-transport->internal raw-schema-req)]
                          (if-not (spec/valid?
                                   ::ss/create-schema-request
                                   internal-sr)
                            (return-error ctx :schema-invalid-request
                                          (spec/explain-data ::ss/create-schema-request
                                                             internal-sr))
                            (do
                              (cs/send-event! communications 
                                              (merge {::cs/event :kixi.datastore/schema-created
                                                      ::cs/version "1.0.0"}
                                                     internal-sr))
                              (assoc (:response ctx)
                                     :status 202
                                     :headers {"Location"
                                               (java.net.URI.
                                                (yada/url-for
                                                 ctx
                                                 :schema-id-entry
                                                 {:route-params {:id new-id}}))})))))}}}))

(defn healthcheck
  [ctx]
                                        ;Return truthy for now, but later check dependancies
  "All is well")

(defn service-routes
  [metrics filestore metadatastore communications schemastore]
  [""
   [["/file" [["" (file-create metrics filestore communications schemastore)]
              [["/" :id] (file-entry metrics filestore metadatastore)]
              [["/" :id "/meta"] (file-meta metrics metadatastore)]
              [["/" :id "/segmentation"] (file-segmentation-create metrics communications metadatastore)]
              [["/" :id "/segmentation/" :segmentation-id] (file-segmentation-entry metrics communications)]
                                        ;              [["/" :id "/segment/" :segment-type "/" :segment-value] (file-segment-entry metrics filestore)]
              ]]
    ["/schema" [[["/"]     (schema-resources metrics schemastore communications)]
                [["/" :id] (schema-id-entry metrics schemastore)]]]]])

(defn routes
  "Create the URI route structure for our application."
  [metrics filestore metadatastore communications schemastore]
  [""
   [(hello-routes metrics)
    (service-routes metrics filestore metadatastore communications schemastore)
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
    [port listener log-config metrics filestore metadatastore communications schemastore]
    component/Lifecycle
    (start [component]
      (if listener
        component                       ; idempotence
        (let [vhosts-model
              (vhosts-model
               [{:scheme :http :host (format "localhost:%d" port)}
                (routes metrics filestore metadatastore communications schemastore)])
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
