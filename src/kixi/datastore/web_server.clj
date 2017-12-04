(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [cheshire.generate :refer [add-encoder]]
            [clojure.core.async :as async :refer [<!!]]
            [clojure.spec.alpha :as spec]
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
            [yada.resources.webjar-resource :refer [new-webjar-resource]]
            [kixi.datastore.metadatastore :as md]))

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

(defn ctx->user
  [ctx]
  {:kixi.user/id (ctx->user-id ctx)
   :kixi.user/groups (ctx->user-groups ctx)})

(defn ctx->request-log
  [ctx]
  (let [req (:request ctx)]
    (str "REQUEST: " {:method (:request-method req)
                      :uri (:uri req)
                      :params (:params req)
                      :query-string (:query-string req)
                      :user-id (ctx->user-id ctx)
                      :user-groups (ctx->user-groups ctx)})))

(comment "under error conditions yada is attempting to log, into json, it's Resource instances, they are
unparsable and don't contain any routing information, so we'll just drop them")
(add-encoder Class
             (fn [^Class c
                  ^com.fasterxml.jackson.core.JsonGenerator jsonGenerator]
               (.writeString jsonGenerator (.getName c))))
(add-encoder Object
             (fn [^Object o
                  ^com.fasterxml.jackson.core.JsonGenerator jsonGenerator]
               (try
                 (.writeObject jsonGenerator o)
                 (catch java.lang.IllegalStateException e
                   (.writeString jsonGenerator (str (type o)))))))

(defn yada-timbre-logger
  [request-logging?]
  (fn [ctx]
    (when request-logging?
      (info (ctx->request-log ctx)))
    ctx))

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
                    :query-count-invalid
                    :query-sort-order-invalid
                    :unauthorised})

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

(defn return-redirect
  [ctx location]
  (assoc (:response ctx)
         :status 302
         :headers {"Location" location}))

(def server-error-resp
  {:msg "Server Error, see logs"})

(defn resource
  [metrics request-logging? model]
  (-> model
      (assoc :logger (yada-timbre-logger request-logging?))
      (assoc :responses {500 {:produces "application/transit+json"
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
  {:id :file-entry
   :methods
   {:get {:produces [{:media-type #{"application/octet-stream"}}]
          :response
          (fn [ctx]
            (let [id (get-in ctx [:parameters :path :id])]
              (if (ms/authorised metadatastore ::ms/file-read id (ctx->user-groups ctx))
                (ds/retrieve filestore
                             id)
                (return-unauthorised ctx))))}}})

(defn file-meta
  [metrics metadatastore]
  {:id :file-meta
   :methods
   {:get {:produces "application/transit+json"
          :response
          (fn [ctx]
            (let [id (get-in ctx [:parameters :path :id])]
              (if (ms/authorised metadatastore ::ms/meta-read id (ctx->user-groups ctx))
                (ms/retrieve metadatastore id)
                (return-unauthorised ctx))))}}})

(defn link-created-event
  [ctx link file-id]
  {::cs/event :kixi.datastore.filestore/download-link-created
   ::cs/version "1.0.0"
   ::cs/partition-key file-id
   ::ms/id file-id
   :kixi/user (ctx->user ctx)
   ::ds/link link})

(defn link-rejected-event
  [ctx file-id]
  {::cs/event :kixi.datastore.filestore/download-link-rejected
   ::cs/version "1.0.0"
   ::cs/partition-key file-id
   :reason :unauthorised
   ::ms/id file-id
   :kixi/user (ctx->user ctx)})

(defn file-download
  [metrics metadatastore filestore communications]
  {:id :file-download
   :methods
   {:get {:produces [{:media-type #{"application/octet-stream"}}]
          :response
          (fn [ctx]
            (let [file-id (get-in ctx [:parameters :path :id])]
              (if (ms/authorised metadatastore ::ms/file-read file-id (ctx->user-groups ctx))
                (let [metadata (ms/retrieve metadatastore file-id)
                      link (ds/create-link
                            filestore
                            file-id
                            (str (::ms/name metadata) "." (::ms/file-type metadata)))]
                  (cs/send-event! communications (link-created-event ctx link file-id))
                  (return-redirect ctx link))
                (do (cs/send-event! communications (link-rejected-event ctx file-id))
                    (return-unauthorised ctx)))))}}})

(defn decode-keyword
  [kw-s]
  (apply keyword
         (clojure.string/split kw-s #"_")))

(def default-query-count "100")

(defn metadata-query
  [metrics metadatastore]
  {:id :file-meta
   :methods
   {:get {:produces "application/transit+json"
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
                  cnt (Integer/parseInt (or (get-in ctx [:parameters :query "count"] default-query-count)))
                  sort-by (vec-if-not
                           (or (get-in ctx [:parameters :query "sort-by"]) [::md/provenance ::md/created]))
                  sort-order (or (get-in ctx [:parameters :query "sort-order"]) "desc")]
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
                ((complement #{"asc" "desc"}) sort-order) (return-error ctx
                                                                        :query-sort-order-invalid
                                                                        "Sort order must be either asc or desc")
                :default (ms/query metadatastore
                                   query
                                   dex cnt
                                   sort-by sort-order))))}}})

(defn file-segmentation-create
  [metrics communications metadatastore]
  {:id :file-segmentation
   :methods
   {:post {:consumes "application/transit+json"
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
                                                 ::cs/partition-key file-id
                                                 :kixi.datastore.request/type ::seg/group-rows-by-column
                                                 ::seg/id id
                                                 ::ms/id file-id
                                                 ::seg/column-name col-name
                                                 :kixi.user/id user-id})))
                   (java.net.URI.
                    (yada/url-for ctx :file-segmentation-entry {:route-params {:segmentation-id id
                                                                               :id file-id}})))
                 (assoc (:response ctx) ;don't know why i'm having to do this here...
                        :status 404))))}}})

(defn file-segmentation-entry
  [metrics filestore]
  {:id :file-segmentation-entry
   :methods
   {:get {:produces "application/transit+json"
          :response
          (fn [ctx]
                                        ;get all the segmentation information (each segment is a FILE!)
            (let [id (get-in ctx [:parameters :path :id])]
              (ds/retrieve filestore
                           id)))}}})

(defn schema-id-entry
  [metrics schemastore]
  {:id :schema-id-entry
   :methods
   {:get {:produces "application/transit+json"
          :response
          (fn [ctx]
            (let [id (get-in ctx [:parameters :path :id])]
              (when (ss/exists schemastore id)
                (if (ss/authorised schemastore ::ss/read id (ctx->user-groups ctx))
                  (ss/retrieve schemastore id)
                  (return-unauthorised ctx)))))}}})

(defn healthcheck
  [ctx]
                                        ;Return truthy for now, but later check dependancies
  (assoc (:response ctx)
         :status 200
         :body "All is well"))

(defn service-routes
  [metrics filestore metadatastore communications schemastore request-logging?]
  [""
   [["/file" [[["/" :id] (resource metrics request-logging? (file-entry metrics filestore metadatastore))]
              [["/" :id "/meta"] (resource metrics request-logging? (file-meta metrics metadatastore))]
              [["/" :id "/link"] (resource metrics request-logging? (file-download metrics metadatastore filestore communications))]
              [["/" :id "/segmentation"] (resource metrics request-logging? (file-segmentation-create metrics communications metadatastore))]
              [["/" :id "/segmentation/" :segmentation-id] (resource metrics request-logging? (file-segmentation-entry metrics communications))]
                                        ;              [["/" :id "/segment/" :segment-type "/" :segment-value] (file-segment-entry metrics filestore)]
              ]]
    ["/metadata" [[["/" :id] (resource metrics request-logging? (file-meta metrics metadatastore))]
                  [[""] (resource metrics request-logging? (metadata-query metrics metadatastore))]]]
    ["/schema" [[["/" :id] (resource metrics request-logging? (schema-id-entry metrics schemastore))]]]]])

(defn routes
  "Create the URI route structure for our application."
  [metrics filestore metadatastore communications schemastore request-logging?]
  [""
   [(service-routes metrics filestore metadatastore communications schemastore request-logging?)

    ["/healthcheck" healthcheck]

    ["/metrics" (yada/resource (:expose-metrics-resource metrics))]

    ;; This is a backstop. Always produce a 404 if we ge there. This
    ;; ensures we never pass nil back to Aleph.
    [true (yada/handler nil)]]])

(defrecord WebServer
    [port listener log-config metrics filestore metadatastore communications schemastore request-logging?]
  component/Lifecycle
  (start [component]
    (if listener
      component
      (let [listener (yada/listener (routes metrics filestore metadatastore communications schemastore request-logging?)
                                    {:port port})]
        (infof "Started web-server on port %s" port)
        (assoc component :listener listener))))
  (stop [component]
    (when-let [close (get-in component [:listener :close])]
      (infof "Stopping web-server on port %s" port)
      (close))
    (assoc component :listener nil)))

(defn new-web-server [config]
  (map->WebServer (:web-server config)))
