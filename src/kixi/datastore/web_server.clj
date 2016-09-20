(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [kixi.datastore.documentstore :as ds]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.communications :as c]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.transit :as t]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [yada             
             [resource :as yr]
             [yada :as yada]]
            [yada.resources.webjar-resource :refer [new-webjar-resource]]
            [kixi.datastore.transit :as t])
  (:import [kixi.datastore.metadatastore DocumentMetaData]))

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
  [documentstore id part]
  (transfer-piece! (ds/output-stream documentstore id) 
                   part
                   true))

(defn file-part?
  [part]
  (= "application/octet-stream" 
     (get-in part [:headers "content-type"])))

(defrecord PartConsumer
    [documentstore]
    yada.multipart/PartConsumer
    (consume-part [_ state part]
      (if (file-part? part)
        (let [id (uuid)]
          (flush-tiny-file! documentstore id part)
          (-> part
              (assoc :id id
                     :count 1
                     :size-bytes (payload-size part))
              (dissoc :bytes)
              (add-part state)))
        (add-part part state)))
    (start-partial [_ piece]
      (let [id (uuid)
            out (ds/output-stream documentstore id)]
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
       DocumentMetaData (fn [part]
                          (ms/map->DocumentMetaData {:id (:id part)
                                                     :pieces-count (:count part)
                                                     :name (get-in part [:content-disposition :params "name"])
                                                     :size-bytes (:size-bytes part)}))}))

(defn file-create
  [metrics documentstore communications]
  (resource
   metrics
   {:id :file-create
    :methods
    {:post
     {:consumes "multipart/form-data"
      :parameters {:body {:file DocumentMetaData
                          :name s/Str}}
      :part-consumer (map->PartConsumer {:documentstore documentstore})
      :response (fn [ctx]
                  (let [params (get-in ctx [:parameters :body])
                        p (merge (:file params)
                                 {:type :csv})]
                    (c/submit-metadata communications p)
                    (java.net.URI. (:uri (yada/uri-for ctx :file-entry {:route-params {:id (:id p)}})))))}}}))

(defn file-entry 
  [metrics documentstore]
  (resource
   metrics
   {:id :file-entry
    :methods
    {:get {:produces [{:media-type #{"application/octet-stream"}}]
           :response
           (fn [ctx]
             (let [id (get-in ctx [:parameters :path :id])]
               (ds/retrieve documentstore 
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

(defn schema-resources
  [metrics schemastore]
  (resource
   metrics
   {:id :schema-create
    :methods
    {:post {:consumes "application/transit+json"
            :response (fn [ctx]                  
                        (let [name (get-in ctx [:parameters :path :name])
                              body (get-in ctx [:body])]
                          (ss/persist schemastore name (:definition body))
                          true))}
     :get {:produces "application/transit+json"
           :response
           (fn [ctx]
             (let [name (get-in ctx [:parameters :path :name])]
               (some->> name
                        (ss/fetch-definition schemastore)
                        (hash-map :definition))))}}}))

(defn healthcheck
  [ctx]
  ;Return truthy for now, but later check dependancies
  "All is well")

(defn service-routes 
  [metrics documentstore metadatastore communications schemastore]
  ["" 
   [["/file" [["" (file-create metrics documentstore communications)]
              [["/" :id] (file-entry metrics documentstore)]
              [["/" :id "/meta"] (file-meta metrics metadatastore)]]]
    ["/schema" [[["/" :name] (schema-resources metrics schemastore)]]]]])

(defn routes
  "Create the URI route structure for our application."
  [metrics documentstore metadatastore communications schemastore]
  [""
   [(hello-routes metrics)
    (service-routes metrics documentstore metadatastore communications schemastore)
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
    [port listener log-config metrics documentstore metadatastore communications schemastore]
    component/Lifecycle
    (start [component]
      (if listener
        component                       ; idempotence
        (let [vhosts-model
              (vhosts-model
               [{:scheme :http :host (format "localhost:%d" port)}
                (routes metrics documentstore metadatastore communications schemastore)])
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
