(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [clojure.java.io :as io]
            [clojure.spec :as spec]
            [clojure.walk :as walk]
            [com.stuartsierra.component :as component]
            [kixi.datastore.filestore :as ds]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.communications :as c]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.validator :as sv]
            [kixi.datastore.segmentation :as seg]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [yada
             [resource :as yr]
             [yada :as yada]]
            [yada.resources.webjar-resource :refer [new-webjar-resource]]
            [kixi.datastore.transit :as t]))

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

(defn resource
  [metrics model]
  (-> model
      (assoc :logger yada-timbre-logger)
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
                        ^int (- (alength ^bytes (:bytes part)) offset))))
     Map (fn [part]
           {:id (:id part)
            :pieces-count (:count part)
            :name (get-in part [:content-disposition :params "name"])
            :size-bytes (:size-bytes part)})}))

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
                          :name s/Str
                          :schema-id s/Str}}
      :part-consumer (map->PartConsumer {:filestore filestore})
      :response (fn [ctx]
                  (let [params (get-in ctx [:parameters :body])
                        file (:file params)
                        metadata {::ms/id (:id file)
                                  ::ss/id (:schema-id params)
                                  ::ms/type "csv"
                                  ::ms/name (:name params)
                                  ::ms/size-bytes (:size-bytes file)
                                  ::ms/provenance {::ms/source "upload"
                                                   ::ms/pieces-count (:pieces-count file)}}]
                    (let [error (or (spec/explain-data ::ms/filemetadata metadata)
                                    (when-not (ss/exists schemastore (::ss/id metadata))
                                      {:error :unknown-schema
                                       :msg (::ss/id metadata)}))]
                      (if error
                        (assoc (:response ctx)
                               :status 400
                               :body error)
                        (do
                          (c/submit communications metadata)
                          (java.net.URI. (:uri (yada/uri-for ctx :file-entry {:route-params {:id (::ms/id metadata)}}))))))))}}}))

(defn file-entry
  [metrics filestore]
  (resource
   metrics
   {:id :file-entry
    :methods
    {:get {:produces [{:media-type #{"application/octet-stream"}}]
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (ds/retrieve filestore
                            id)))}}}))

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
               (ms/fetch metadatastore id)))}}}))

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
                    type (:type body)]
                (if (ms/exists metadatastore file-id)
                  (do
                    (case type
                      "column" (let [col-name (:column-name body)]
                                 (c/submit communications
                                           {:kixi.datastore.request/type ::seg/group-rows-by-column
                                            ::seg/id id
                                            ::ms/id file-id
                                            ::seg/column-name col-name})))
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
    {:get {:produces "application/transit+json"
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (ss/fetch-with schemastore {::ss/id id})))}}}))

(defn return-error
  [ctx msg error-key]
  (assoc (:response ctx)
         :status 400
         :body {:error error-key
                :msg msg}))

(defn add-ns-to-keywords
  ([ns m]
   (letfn [(process [n]
             (if (= (type n) clojure.lang.MapEntry)
               (clojure.lang.MapEntry. (keyword (namespace ns) (name (first n))) (second n))
               n))]
     (walk/prewalk process m))))

(defn schema-resources
  [metrics schemastore communications]
  (resource
   metrics
   {:id :schema-create
    :methods
    {:post {:consumes "application/transit+json"
            :produces "application/transit+json"
            :response (fn [ctx]
                        (let [new-id      (uuid)
                              body        (get-in ctx [:body])
                              schema      (add-ns-to-keywords ::ss/_ (:schema body))
                              schema'     (dissoc schema ::ss/name)
                              schema-name (::ss/name schema)]
                          ;; Is name valid?
                          (if-let [error (sv/invalid-name? schema-name)]
                            (return-error ctx :schema-invalid-name error)
                            ;; Is definition valid?
                            (if-let [error (sv/invalid-schema? schema')]
                              (return-error ctx :schema-invalid-definition error)
                              ;; Does this name + definition already exist?
                              (if-let [preexists (ss/fetch-with schemastore {::ss/name schema-name
                                                                             ::ss/schema schema'})]
                                (assoc (:response ctx)
                                       :status 202 ;; wants to be a 303
                                       :headers {"Location"
                                                 (str (java.net.URI.
                                                       (:uri (yada/uri-for
                                                              ctx
                                                              :schema-id-entry
                                                              {:route-params {:id (::ss/id preexists)}}))))})
                                (do
                                  (c/submit communications
                                            {::ss/name schema-name
                                             ::ss/schema schema'
                                             ::ss/id new-id})
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
  [metrics filestore metadatastore communications schemastore]
  [""
   [["/file" [["" (file-create metrics filestore communications schemastore)]
              [["/" :id] (file-entry metrics filestore)]
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
