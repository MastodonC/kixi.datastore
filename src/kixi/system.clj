(ns kixi.system
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [com.stuartsierra.component :refer [system-map system-using] :as component]
            [taoensso.timbre :as log])
  (:require [kixi.web-server :as web-server]))

(def logback-timestamp-opts
  {:pattern  "yyyy-MM-dd HH:mm:ss,SSS"
   :locale   :jvm-default
   :timezone :utc})

(def upper-name 
  (memoize 
   (fn [level]
     (str/upper-case (name level)))))

(defn output-fn
  "Default (fn [data]) -> string output fn.
  Use`(partial default-output-fn <opts-map>)` to modify default opts."
  ([data] (output-fn nil data))
  ([opts data] ; For partials
   (let [{:keys [no-stacktrace? stack-fonts]} opts
         {:keys [level ?err #_vargs msg_ ?ns-str hostname_
                 timestamp_ ?line]} data]
     (str
      (force timestamp_)  " "
      (upper-name level)  " "
      "[" (or ?ns-str "?") ":" (or ?line "?") "] - "
      (force msg_)
      (when-not no-stacktrace?
        (when-let [err ?err]
          (str "\n" (log/stacktrace err opts))))))))

(defn configure-logging!
  [config]
  (let [full-config (merge (:logging config)
                           {:timestamp-opts logback-timestamp-opts ; iso8601 timestamps
                            :output-fn (partial output-fn {:stacktrace-fonts {}})})]
    (log/merge-config! full-config)
    (log/handle-uncaught-jvm-exceptions! 
     (fn [throwable ^Thread thread]
       (log/error throwable (str "Unhandled exception on " (.getName thread)))))))

(defn config
  "Read EDN config, with the given profile. See Aero docs at
  https://github.com/juxt/aero for details."
  [profile]
  (aero/read-config (io/resource "config.edn") {:profile profile}))

(defn component-dependencies
  []
  {})

(defn new-system-map
  [config]
  (system-map
   :web-server (web-server/new-web-server config)))

(defn configure-components
  "Merge configuration to its corresponding component (prior to the
  system starting). This is a pattern described in
  https://juxt.pro/blog/posts/aero.html"
  [system config]
  (merge-with merge system config))

(defn new-system
  [profile]
  (let [config (config profile)
        _ (configure-logging! config)]
    (-> (new-system-map config)
        (configure-components config)
        (system-using (component-dependencies)))))

(defn -main 
  [& args]
  (component/start-system
   (new-system (first args))))
