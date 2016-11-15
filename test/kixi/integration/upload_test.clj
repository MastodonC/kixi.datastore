(ns kixi.integration.upload-test
  (:require [byte-streams :as bs]
            [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.core.async :as async :refer [go]]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [kixi.integration.base :refer :all]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.datastore.system :as system]
            [kixi.repl :as repl]
            [kixi.datastore.filestore :as fs]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [kixi.repl :as repl])
  (:import [java.io
            File
            FileNotFoundException]))


(def uid (uuid))

(def irrelevant-schema-id (atom nil))
(def irrelevant-schema {:name ::irrelevant-schema
                        :schema {:type "list"
                                 :definition [:cola {:type "integer"}
                                              :colb {:type "integer"}]
                                 :sharing {:read [uid]
                                           :use [uid]}}})

(defn setup-schema
  []
  (let [r (post-spec-and-wait uid irrelevant-schema)]
    (if (= 202 (:status r))
      (reset! irrelevant-schema-id (extract-id r))
      (throw (Exception. "Couldn't post irrelevant-schema")))))

(deftest round-trip-files
  (try 
    (repl/start)
    (setup-schema)
    (let [r (post-file uid
                       "./test-resources/metadata-one-valid.csv"
                       @irrelevant-schema-id)]
      (is (= 201
             (:status r))
          (str "Reason: " (parse-json (:body r))))
      (when-let [locat (get-in r [:headers "Location"])]
        (is (files-match?
             "./test-resources/metadata-one-valid.csv"
             (dload-file uid locat)))))
    (let [r (post-file uid
                       "./test-resources/metadata-12MB-valid.csv"
                       @irrelevant-schema-id)]
      (is (= 201
             (:status r))
          (str "Reason: " (parse-json (:body r))))
      (when-let [locat (get-in r [:headers "Location"])]
        (is (files-match?
             "./test-resources/metadata-12MB-valid.csv"
             (dload-file uid locat)))))
    #_(let [r (post-file uid
                         "./test-resources/metadata-344MB-valid.csv"
                         @irrelevant-schema-id)]
        (is (= 201
               (:status r))
            (str "Reason: " (parse-json (:body r))))
        (when-let [locat (get-in r [:headers "Location"])]
          (is (files-match?
               "./test-resources/metadata-344MB-valid.csv"
               (dload-file uid locat)))))
    (finally
      (repl/stop))))

(defrecord OutputStreamsFailAfterX
    [x]
    fs/FileStore
    (exists [this id]
      (throw (new IllegalAccessException)))
    (output-stream [this id content-length]
      [(go :done)
       (proxy [java.io.OutputStream] []
         (write [array start end]
           (if (pos? @x)
             (swap! x dec)
             (throw (new java.io.IOException)))))])
    (retrieve [this id]
      (throw (new IllegalAccessException)))

    component/Lifecycle
    (start [component]
      component)
    (stop [component]
      component))


(defrecord ExceptionInChan
    []
    fs/FileStore
    (exists [this id]
      (throw (new IllegalAccessException)))
    (output-stream [this id content-length]
      [(go (throw (new java.io.IOException)))
       (proxy [java.io.OutputStream] []
         (write [array start end]
           true))])
    (retrieve [this id]
      (throw (new IllegalAccessException)))

    component/Lifecycle
    (start [component]
      component)
    (stop [component]
      component))


(deftest upload-output-stream-immediate-failure
  (let [system (-> (system/new-system :local)
                   (assoc :filestore
                          (map->OutputStreamsFailAfterX {:x (atom 0)}))
                   component/start-system)]
    (setup-schema)
    (try
      (let [r (post-file uid
                         "./test-resources/metadata-one-valid.csv"
                         @irrelevant-schema-id)]
        (is (= 500
               (:status r))
            (str "Reason: " (parse-json (:body r)))))
      (comment "This is what should happen with a larger than 1 chunk file"
               (let [r (post-file uid
                                  "./test-resources/metadata-12MB-valid.csv"
                                  @irrelevant-schema-id)]
                 (is (= 500
                        (:status r))
                     (str "Reason: " (parse-json (:body r))))))
      (comment "This is what currently occurs:")
      (is (thrown? java.net.SocketException
                   (post-file uid
                              "./test-resources/metadata-12MB-valid.csv"
                              @irrelevant-schema-id)))
      (finally
        (component/stop-system system)))))


(deftest upload-output-stream-failure-after-2-writes
  (let [system (-> (system/new-system :local)
                   (assoc :filestore
                          (map->OutputStreamsFailAfterX {:x (atom 2)}))
                   component/start-system)]
    (setup-schema)
    (try
      (let [r (post-file uid
                         "./test-resources/metadata-one-valid.csv"
                         @irrelevant-schema-id)]
        (is (= 201
               (:status r))
            (str "Created, as the tiny file will go through in one write")))
      (comment "This is what should happen with a larger than 1 chunk file"
               (let [r (post-file uid
                                  "./test-resources/metadata-12MB-valid.csv"
                                  @irrelevant-schema-id)]
                 (is (= 500
                        (:status r))
                     (str "Reason: " (parse-json (:body r))))))
      (comment "This is what currently occurs:")
      (is (thrown? java.net.SocketException
                   (post-file uid
                              "./test-resources/metadata-12MB-valid.csv"
                              @irrelevant-schema-id)))
      (finally
        (component/stop-system system)
        (Thread/sleep 5000)
        (prn "done")))))

(comment "The 12mb file gets uploaded in 210 parts, test what happens when the stream dies on the final chunk")
(deftest upload-output-stream-failure-after-209-writes
  (let [system (-> (system/new-system :local)
                   (assoc :filestore
                          (map->OutputStreamsFailAfterX {:x (atom 209)}))
                   component/start-system)]
    (setup-schema)
    (try
      (let [r (post-file uid
                         "./test-resources/metadata-12MB-valid.csv"
                         @irrelevant-schema-id)]
        (is (= 500
               (:status r))
            (str "Reason: " (parse-json (:body r)))))
      (finally
        (component/stop-system system)))))

(deftest upload-channel-exception
  (let [system (-> (system/new-system :local)
                   (assoc :filestore
                          (map->ExceptionInChan {}))
                   component/start-system)]
    (setup-schema)
    (try
      (let [r (post-file uid
                         "./test-resources/metadata-12MB-valid.csv"
                         @irrelevant-schema-id)]
        (is (= 500
               (:status r))
            (str "Reason: " (parse-json (:body r)))))
      (finally
        (component/stop-system system)))))
