(ns kixi.datastore.web-server
  (:require [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [yada
             [consume :refer [save-to-file]]
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
     {:parameters {:query {:p String}}
      :produces "text/plain"
      :response say-hello}}}))

(defn file-delivery-resource
  [metrics]
  (resource
   metrics
   {:methods
    {:post
     {:consumes "application/octet-stream"
      :consumer (fn [ctx _ body-stream]
                  (let [f (java.io.File/createTempFile "yada" ".tmp" (io/file "/tmp"))]
                    (infof "Saving to file: %s" f)
                    (save-to-file
                     ctx body-stream
                     f)))
      :response (fn [ctx] (format "Thank you, saved upload content to file: %s\n" (:file ctx)))}}}))

(defn healthcheck
  [ctx]
  ;Return truthy for now, but later check dependancies
  "All is well")

(defn service-routes 
  [metrics]
  ["" [["/hello" (yada/handler "Hello World!\n")]
       ["/hello-param" (yada/handler (hello-parameters-resource metrics))]
       ["/file" (yada/handler (file-delivery-resource metrics))]]])

(defn routes
  "Create the URI route structure for our application."
  [metrics]
  (let [roots (service-routes metrics)]
    [""
     [
      roots
      ["/healthcheck" (yada/handler healthcheck)]
      
      ["/api" (-> roots
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
    [port listener log-config metrics]
    component/Lifecycle
    (start [component]
      (if listener
        component                       ; idempotence
        (let [vhosts-model
              (vhosts-model
               [{:scheme :http :host (format "localhost:%d" port)}
                (routes metrics)])
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
