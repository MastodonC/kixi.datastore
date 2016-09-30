(ns kixi.datastore.transit
  (:require [cognitect.transit :as transit])
  (:import [java.io ByteArrayInputStream
            ByteArrayOutputStream]
           [java.util.regex Pattern]))

(def write-handlers
  {:handlers {Pattern (transit/write-handler "java.util.regex.Pattern" str)}})

(def read-handlers
  {:handlers {"java.util.regex.Pattern" (transit/read-handler #(re-pattern %))}})

(defn clj-form->json-str
  [clj]
  (let [out (ByteArrayOutputStream. 4096)
        wr (transit/writer out :json write-handlers)
        _ (transit/write wr clj)]
    (.toString out)))

(defn json-str->clj-form
  [^String json-str]
  (let [in (ByteArrayInputStream. (.getBytes json-str))]
    (transit/read
     (transit/reader in :json read-handlers))))
