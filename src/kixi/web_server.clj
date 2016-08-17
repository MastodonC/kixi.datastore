(ns kixi.web-server
  (require [clojure.java.io :as io]
           [bidi.bidi :refer [tag]]
           [bidi.vhosts :refer [make-handler vhosts-model]]
           [com.stuartsierra.component :as component]
           [taoensso.timbre :as timbre :refer [infof]]
           [yada.yada :as yada :refer [resource]]
           [yada.consume :refer [save-to-file]]
           [yada.resources.webjar-resource :refer [new-webjar-resource]]))

(defn say-hello [ctx]
  (str "Hello " (get-in ctx [:parameters :query :p]) "!\n"))

(def hello-parameters-resource
  (resource
    {:methods
      {:get
        {:parameters {:query {:p String}}
         :produces "text/plain"
         :response say-hello}}}))


(def file-delivery-resource
  (resource
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

(defn bidi-routes []
  ["" [["/hello" (yada/handler "Hello World!\n")]
       ["/hello-param" (yada/handler hello-parameters-resource)]
       ["/file" (yada/handler file-delivery-resource)]]])

(defn routes
  "Create the URI route structure for our application."
  [config]
  [""
   [
    (bidi-routes)
    
    ["/api" (-> (bidi-routes)
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

    ;; Swagger UI
    ["/swagger" (-> (new-webjar-resource "/swagger-ui" {})
                    ;; Tag it so we can create an href to the Swagger UI
                    (tag :edge.resources/swagger))]

    ;; This is a backstop. Always produce a 404 if we ge there. This
    ;; ensures we never pass nil back to Aleph.
    [true (yada/handler nil)]]])


(defrecord WebServer [port listener]
  component/Lifecycle
  (start [component]
    (if listener
      component                         ; idempotence
      (let [vhosts-model
            (vhosts-model
             [{:scheme :http :host (format "localhost:%d" port)}
              (routes {:port port})])
            listener (yada/listener vhosts-model {:port port})]
        (infof "Started web-server on port %s" listener)
        (assoc component :listener listener))))
  (stop [component]
    (when-let [close (get-in component [:listener :close])]
      (close))
    (assoc component :listener nil)))

(defn new-web-server [config]
  (map->WebServer (:web-server config)))
