(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [byte-streams :as bs]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [kixi.datastore.protocols :as protocols]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [yada             
             [resource :as yr]
             [yada :as yada]]
            [yada.resources.webjar-resource :refer [new-webjar-resource]]))

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

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn add-part
  [part state]
  (update state :parts (fnil conj []) 
          (yada.multipart/map->DefaultPart part)))

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

(defrecord StreamingPartial [id output-stream initial]
  yada.multipart/Partial
  (continue [this piece] 
    (transfer-piece! output-stream piece (:body-offset piece))
    (update this :pieces-count (fnil inc 0)))
  (complete [this state piece]
    (transfer-piece! output-stream piece true)
    (-> initial
        (assoc :type :part
               :count (:pieces-count this)
               :id id)
        (add-part state))))

(defn flush-tiny-file!
  "Files smaller than the buffer size (?) come through as complete parts"
  [documentstore id part]
  (transfer-piece! (protocols/output-stream documentstore {:name id}) 
                   part
                   true))

(defn file-part?
  [part]
  (= "application/octet-stream" 
     (get-in part [:headers "content-type"])))

(defrecord DocumentMeta
    [id pieces-count name])

(defrecord PartConsumer
    [documentstore]
    yada.multipart/PartConsumer
    (consume-part [_ state part]
      (if (file-part? part)
        (let [id (uuid)]
          (flush-tiny-file! documentstore id part)         
          (-> part
              (assoc :id id
                     :count 1)
              (dissoc :bytes)
              (add-part state)))
        (add-part part state)))
    (start-partial [_ piece]
      (let [id (uuid)
            out (protocols/output-stream documentstore {:name id})]
        (transfer-piece! out piece)
        (->StreamingPartial id out (dissoc piece :bytes))))
    (part-coercion-matcher [s]
      "Return a map between a target type and the function that coerces this type into that type"
      {s/Str (fn [part]
               (let [offset (or (:body-offset part) 0)]
                 (String. ^bytes (:bytes part) 
                          ^int offset 
                          ^int (- (alength ^bytes (:bytes part)) offset))))
       DocumentMeta (fn [part]
                      (DocumentMeta. (:id part)
                                     (:count part)
                                     (get-in part [:content-disposition :params "name"])))}))

(defn file-delivery-resource
  [metrics documentstore]
  (resource
   metrics
   {:methods
    {:post
     {:consumes "multipart/form-data"
      :parameters {:body {:file DocumentMeta
                          :name s/Str}}
      :part-consumer (map->PartConsumer {:documentstore documentstore})
      :response (fn [ctx]
                  (let [params  (get-in ctx [:parameters :body])]
                    (info "Params: " params)
                    (format "Thank you, saved upload content to file: %s\n" params)))}}}))

(defn healthcheck
  [ctx]
  ;Return truthy for now, but later check dependancies
  "All is well")

(defn service-routes 
  [metrics documentstore]
  ["" [["/hello" (yada/handler "Hello World!\n")]
       ["/hello-param" (yada/handler (hello-parameters-resource metrics))]
       ["/file" (yada/handler (file-delivery-resource metrics documentstore))]]])

(defn routes
  "Create the URI route structure for our application."
  [metrics documentstore]
  (let [roots (service-routes metrics documentstore)]
    [""
     [
      roots
      ["/healthcheck" (yada/handler healthcheck)]
      
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

      ["/metrics" (yada/handler (yada/resource (:expose-metrics-resource metrics)))]

      ;; Swagger UI
      ["/swagger" (-> (new-webjar-resource "/swagger-ui" {})
                      ;; Tag it so we can create an href to the Swagger UI
                      (tag :edge.resources/swagger))]

      ;; This is a backstop. Always produce a 404 if we ge there. This
      ;; ensures we never pass nil back to Aleph.
      [true (yada/handler nil)]]]))


(defrecord WebServer 
    [port listener log-config metrics documentstore]
    component/Lifecycle
    (start [component]
      (if listener
        component                       ; idempotence
        (let [vhosts-model
              (vhosts-model
               [{:scheme :http :host (format "localhost:%d" port)}
                (routes metrics documentstore)])
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
