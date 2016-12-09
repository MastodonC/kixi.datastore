(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.spec :as spec]
            [com.stuartsierra.component :as component]
            [kixi.datastore
             [communication-specs :as cs]
             [filestore :as ds]
             [metadatastore :as ms]
             [schemastore :as ss]
             [segmentation :as seg]
             [time :as t]
             [transport-specs :as ts]]
            [kixi.datastore.schemastore.conformers :as conformers]
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
    (if-let [err (or (get-in ctx [:response :error])
                     (:error ctx))]
      (if (instance? Exception err)
        (error err "Server error")
        (error (str "Server error: " err))) ;this is unlikely to be used, but straw grasping right now
      (if (or ((set (keys (get-in ctx [:response]))) :error)
              ((set (keys ctx)) :error))
        (error "Server error, error key available, but set to nil")
        (error "Server error, no exception available"))))
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

(def server-error-resp
  {:msg "Server Error, see logs"})

(defn resource
  [metrics model]
  (-> model
      (assoc :logger yada-timbre-logger)
      (assoc :responses {500 {:produces "application/json"
                              :response server-error-resp}})
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

(defrecord StreamingPartial [id ^java.io.OutputStream output-stream complete-chan initial size-bytes pieces-count]
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
               :count ((fnil inc 1) (:pieces-count this))
               :id id
               :size-bytes (+ size-bytes
                              (payload-size piece))
               :complete (<!! complete-chan))
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

(defn file-size
  [piece]
  (let [^String fs (get-in piece [:request-headers "file-size"])]
    (Long/valueOf fs)))

(defrecord PartConsumer
    [filestore]
    yada.multipart/PartConsumer
    (consume-part [this state part]
      (if (file-part? part)
        (let [id (uuid)
              complete-chan-r (flush-tiny-file! filestore id part (file-size part))]
          (-> part
              (assoc :id id
                     :count 1
                     :size-bytes (payload-size part)
                     :complete complete-chan-r)
              (dissoc :bytes)
              (add-part state)))
        (add-part part state)))
    (start-partial [this piece]
      (let [id (uuid)
            [complete-chan out] (ds/output-stream filestore id (file-size piece))]
        (transfer-piece! out piece)
        (->StreamingPartial id out complete-chan
                            (dissoc piece :bytes)
                            (payload-size piece)
                            1))))

(defn rethrow
  [e]
  (if (instance? Exception e)
    (throw e)
    (throw (ex-info "Complete chan must either return exception or :done." {:actual e}))))

(defn file-create
  [metrics filestore communications schemastore]
  (resource
   metrics
   {:id :file-create
    :parameters {:header {(s/required-key "file-size") s/Str
                          (s/required-key "user-id") (s/pred conformers/uuid?)
                          (s/required-key "user-groups") s/Str}}
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
                        declared-file-size (Long/valueOf (str (get-in ctx [:request :headers :file-size])))
                        transported-metadata (json/parse-string (:file-metadata params) keyword)
                        file-details {::ms/id (:id file)
                                      ::ms/size-bytes (:size-bytes file)
                                      ::ms/provenance {::ms/source "upload"
                                                       ::ms/pieces-count (:count file)
                                                       :kixi.user/id (ctx->user-id ctx)
                                                       ::ms/created (t/timestamp)}}
                        metadata (ts/filemetadata-transport->internal
                                  transported-metadata
                                  file-details)
                        explained (spec/explain-data ::ms/file-metadata metadata)
                        schema-id (get-in metadata [::ms/schema ::ss/id])]
                    (cond
                      (not= declared-file-size (:size-bytes file)) (return-error ctx                                               
                                                                                 {::error :file-upload-failed
                                                                                  ::msg {:declared-size declared-file-size
                                                                                         :received-size (:size-bytes file)}})
                      (not= :done (:complete file)) (rethrow (:complete file))
                      explained (return-error ctx explained)
                      (and schema-id
                           (not (ss/exists schemastore schema-id))) (return-error ctx 
                           {::error :unknown-schema
                            ::msg {:schema-id schema-id}})
                      (and schema-id
                           (not (ss/authorised schemastore
                                               ::ss/use
                                               schema-id
                                               (ctx->user-groups ctx)))) (return-unauthorised ctx)
                      :else (do
                              (cs/send-event! communications
                                              (assoc metadata
                                                     ::cs/event :kixi.datastore/file-created
                                                     ::cs/version "1.0.0"))
                              (cs/send-event! communications {::cs/event :kixi.datastore/file-metadata-updated
                                                              ::cs/version "1.0.0"
                                                              ::cs/file-metadata-update-type 
                                                              ::cs/file-metadata-created
                                                              ::ms/file-metadata metadata})
                              (java.net.URI.
                               (yada/url-for ctx :file-entry {:route-params {:id (::ms/id metadata)}}))))))}}}))

(defn file-shunt
  "Experimental non-multipart file upload resource"
  [metrics filestore communications schemastore]
  (resource
   metrics
   {:id :file-shunt   
    :parameters {:header {(s/required-key "file-size") s/Str}}
    :methods
    {:post
     {:consumes "application/x-www-form-urlencoded"
      :produces "application/json"
      :consumer (fn [ctx _ body-stream]
                  (let [id (uuid)
                        file-size (let [^String fs (get-in ctx [:request :headers "file-size"])]
                                    (Long/valueOf fs))
                        [complete-chan out] (ds/output-stream filestore id file-size)]
                    (bs/transfer body-stream
                                 out)
                    (<!! complete-chan)
                    ctx))
      :response (fn [ctx]
                  "Thanks")}}}))

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
  (assoc (:response ctx)
         :status 200
         :body "All is well"))

(defn service-routes
  [metrics filestore metadatastore communications schemastore]
  [""
   [["/file" [["" (file-create metrics filestore communications schemastore)]
              ["/shunt"
               (file-shunt metrics filestore communications schemastore)]
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
    [port vhost listener log-config metrics filestore metadatastore communications schemastore]
    component/Lifecycle
    (start [component]
      (if listener
        component
        (let [vhosts-model (vhosts-model
                            [{:scheme :http 
                              :host (str vhost ":" port)}
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
