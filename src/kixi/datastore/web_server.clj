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

(defrecord PartConsumer
    [documentstore]
    yada.multipart/PartConsumer
    (consume-part [_ state part]
      "Return state with part attached"
      (info "Part: " part)
      (if (= "file" (get-in part [:content-disposition :params "name"]))
        (let [out (get-in state [:output-stream] (protocols/output-stream documentstore {:name "dupido"}))]
          (bs/transfer (:bytes part) out)
          (-> state
              (assoc :output-stream out)
              (update :parts (fnil conj []) (yada.multipart/map->DefaultPart (dissoc part :bytes))))) ;hopefully clear the bytes, do a BIG test"
        (update state :parts (fnil conj []) (yada.multipart/map->DefaultPart part))))
    (start-partial [_ piece]
      "Return a partial"
      (info "Partial")
      (yada.multipart/->DefaultPartial piece))
    (part-coercion-matcher [s]
      "Return a map between a target type and the function that coerces this type into that type"
      (info "coercion: " s)
      {s/Str (fn [part]
               (info "C " part)
               (let [offset (get part :body-offset 0)]
                 (String. (:bytes part) offset (- (count (:bytes part)) offset))))
       s/Any (constantly "ASd")}))

(defn file-delivery-resource
  [metrics documentstore]
  (resource
   metrics
   {:methods
    {:post
     {:consumes "multipart/form-data"
      :parameters {:body {:file s/Any
                          :name s/Str}}
      :part-consumer (map->PartConsumer {:documentstore documentstore})
      :response (fn [ctx] 
                  (info "Params: " (get-in ctx [:parameters :body]))
                  (format "Thank you, saved upload content to file: %s\n" ctx))}}}))

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
