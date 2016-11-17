(ns kixi.datastore.elasticsearch
  (:require [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest
             [document :as esd]
             [index :as esi]]
            [taoensso.timbre :as timbre :refer [error]]))

(def put-opts {:consistency "default"})

(def string-stored-not_analyzed
  {:type "string"
   :store "yes"
   :index "not_analyzed"})

(def string-analyzed
  {:type "string"
   :store "yes"})

(def timestamp
  {:type "string"})

(def number
  {:type "string"})

(defn kw->es-format
  [kw]
  (if (namespace kw)
    (str (clojure.string/replace (namespace kw) "." "_")
         "__"
         (name kw))
    (name kw)))

(defn es-format->kw
  [confused-kw]
  (let [splits (clojure.string/split (name confused-kw) #"__")]
    (if (second splits)
      (keyword
       (clojure.string/replace (first splits) "_" ".")
       (second splits))
      confused-kw)))

(defn map-all-keys
  [f]
  (fn mapper [m]
    (cond
      (map? m) (zipmap (map f (keys m))
                       (map mapper (vals m)))
      (list? m) (map mapper m)
      (vector? m) (mapv mapper m)
      (seq? m) (str m) ; (map mapper m) spec errors contain seq's of sexp's containing code, which breaks elasticsearch validation.
      (symbol? m) (name m)
      (keyword? m) (f m)
      :else m)))

(def all-keys->es-format
  (map-all-keys kw->es-format))

(def all-keys->kw
  (map-all-keys es-format->kw))

(defn get-document-raw
  [index-name doc-type conn id]
  (esd/get conn
           index-name 
           doc-type 
           id
           {:preference "_primary"}))

(defn get-document
  [index-name doc-type conn id]  
  (-> (get-document-raw index-name doc-type conn id)
      :_source
      all-keys->kw))

(defn get-document-key
  [index-name doc-type conn id k]
  (-> (get-document-raw index-name doc-type conn id)
      :_source
      (get (keyword (kw->es-format k)))
      all-keys->kw))

(defn ensure-index
  [index-name doc-type doc-def conn]
  (when-not (esi/exists? conn index-name)
    (esi/create conn 
                index-name
                {:mappings {doc-type 
                            {:properties (all-keys->es-format doc-def)}}
                 :settings {}})))

(def apply-attempts 10)

(defn version-conflict?
  [resp]
  (some
   #(= "version_conflict_engine_exception"
       (:type %))
   ((comp :root_cause :error) resp)))

(defn error?
  [resp]
  (:error resp))

(defn apply-func
  ([index-name doc-type conn id f]
   (loop [tries apply-attempts]
     (let [curr (get-document-raw index-name doc-type conn id)
           resp (esd/put conn
                         index-name
                         doc-type
                         id
                         (f curr) 
                         (merge put-opts
                                (when (:_version curr)
                                  {:version (:_version curr)})))]
       (if (and (version-conflict? resp)
                (pos? tries))
         (recur (dec tries))
         resp)))))

(defn merge-data
  [index-name doc-type conn id update]
  (let [es-u (all-keys->es-format update)
        r (apply-func
           index-name
           doc-type
           conn
           id   
           (fn [curr]
             (merge-with merge
                         (:_source curr)
                         es-u)))]
    (if (error? r)
      (error "Unable to merge data for id: " id ". Trying to merge: " es-u ". Response: " r)
      r)))

(defn cons-data
  [index-name doc-type conn id k element]
  (let [r (apply-func
           index-name
           doc-type
           conn
           id   
           (fn [curr]
             (update (:_source curr) (kw->es-format k) 
                     #(cons (all-keys->es-format element) %))))]
    (if (error? r)
      (error "Unable to cons data for id: " id ". Trying to update: " k ". Response: " r)
      r)))

(defn present?
  [index-name doc-type conn id]
  (esd/present? conn index-name doc-type id))

(defn connect
  [host port]
  (esr/connect (str "http://" host ":" port)
               {:connection-manager (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 10})}))
