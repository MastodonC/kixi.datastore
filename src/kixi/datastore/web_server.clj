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
  (some-> (get-in ctx [:request :headers "user-groups"])
          (clojure.string/split #",")
          vec-if-not))

(defn yada-timbre-logger
  [ctx]
  (when (= 500 (get-in ctx [:response :status]))
    (if-let [err (or (get-in ctx [:response :error])
                     (:error ctx))]
      (if (instance? Exception err)
        (error err "Server error")
        (error (str "Server error: " err)))
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
                    :unknown-schema :file-upload-failed
                    :query-invalid :query-index-invalid
                    :query-count-invalid})

(spec/def ::msg (spec/or :error-map (spec/keys :req []
                                    :opts [])
                         :error-str string?))

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

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

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
               (if (ms/authorised metadatastore ::ms/file-read id (ctx->user-groups ctx))
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
               (if (ms/authorised metadatastore ::ms/meta-read id (ctx->user-groups ctx))
                 (ms/retrieve metadatastore id)
                 (return-unauthorised ctx))))}}}))

(defn decode-keyword
  [kw-s]
  (apply keyword
         (clojure.string/split kw-s #"_")))

(def default-query-count "100")

(defn metadata-query
  [metrics metadatastore]
  (resource
   metrics
   {:id :file-meta
    :methods
    {:get {:produces "application/json"
           :response
           (fn [ctx]
             (let [user-groups (ctx->user-groups ctx)
                   activities (->> (get-in ctx [:parameters :query "activity"])
                                   vec-if-not
                                   (mapv decode-keyword))
                   query {:kixi.user/groups user-groups
                          ::ms/activities activities}
                   explain (spec/explain-data ::ms/query-criteria query)
                   dex (Integer/parseInt (or (get-in ctx [:parameters :query "index"]) "0"))
                   cnt (Integer/parseInt (or (get-in ctx [:parameters :query "count"] default-query-count)))]
               (cond 
                 explain (return-error ctx
                                       :query-invalid
                                       explain)
                 (neg? dex) (return-error ctx
                                          :query-index-invalid
                                          "Index must be positive")
                 (neg? cnt) (return-error ctx
                                          :query-count-invalid
                                          "Count must be positive")
                 :default (ms/query metadatastore
                                     query
                                     dex cnt))))}}}))

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

(defn healthcheck
  [ctx]
                                        ;Return truthy for now, but later check dependancies
  (assoc (:response ctx)
         :status 200
         :body "All is well"))

(defn service-routes
  [metrics filestore metadatastore communications schemastore]
  [""
   [["/file" [[["/" :id] (file-entry metrics filestore metadatastore)]
              [["/" :id "/meta"] (file-meta metrics metadatastore)]
              [["/" :id "/segmentation"] (file-segmentation-create metrics communications metadatastore)]
              [["/" :id "/segmentation/" :segmentation-id] (file-segmentation-entry metrics communications)]
                                        ;              [["/" :id "/segment/" :segment-type "/" :segment-value] (file-segment-entry metrics filestore)]
              ]]
    ["/metadata" [[["/" :id] (file-meta metrics metadatastore)]
                  [[""] (metadata-query metrics metadatastore)]]]
    ["/schema" [[["/" :id] (schema-id-entry metrics schemastore)]]]]])

(defn routes
  "Create the URI route structure for our application."
  [metrics filestore metadatastore communications schemastore]
  [""
   [(service-routes metrics filestore metadatastore communications schemastore)  
 
    ["/healthcheck" healthcheck]

    ["/metrics" (yada/resource (:expose-metrics-resource metrics))]

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
