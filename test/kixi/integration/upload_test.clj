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

(deftest round-trip-files
  (try 
    (repl/start)
    (let [r (post-file uid
                       "./test-resources/metadata-one-valid.csv")]
      (is (= 201
             (:status r))
          (str "Reason: " (parse-json (:body r))))
      (when-let [locat (get-in r [:headers "Location"])]
        (is (files-match?
             "./test-resources/metadata-one-valid.csv"
             (dload-file uid locat)))))
    (let [r (post-file uid
                       "./test-resources/metadata-12MB-valid.csv")]
      (is (= 201
             (:status r))
          (str "Reason: " (parse-json (:body r))))
      (when-let [locat (get-in r [:headers "Location"])]
        (is (files-match?
             "./test-resources/metadata-12MB-valid.csv"
             (dload-file uid locat)))))
    (let [r (post-file uid
                       "./test-resources/metadata-344MB-valid.csv")]
      (is (= 201
             (:status r))
          (str "Reason: " (parse-json (:body r))))
      (when-let [locat (get-in r [:headers "Location"])]
        (is (files-match?
             "./test-resources/metadata-344MB-valid.csv"
             (dload-file uid locat)))))
    (finally
      (repl/stop))))

(def caller-depth
  {"consume_part" 8
   "start_partial" 6
   "continue" 6
   "complete" 6})

(defn called-from?
  [target]
  (let [depth (get caller-depth target)]
    (fn []
      (let [e (new Exception)
            st (.getStackTrace e)
            ^StackTraceElement ele (nth st depth)]
        (= target
           (.getMethodName ele))))))

(defrecord OutputStreamsFailWhenCalledFrom
    [called-from]
    fs/FileStore
    (exists [this id]
      (throw (new IllegalAccessException)))
    (output-stream [this id content-length]
      [(go :done)
       (proxy [java.io.OutputStream] []
         (write [array start end]
           (when (called-from)
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
      [(go (new java.io.IOException))
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


(deftest upload-small-file-with-IOException-on-consume_part
  (let [system (repl/start {:filestore
                            (OutputStreamsFailWhenCalledFrom. (called-from? "consume_part"))})]
    (try
      (let [r (post-file uid
                         "./test-resources/metadata-one-valid.csv")]
        (is (= 500
               (:status r))
            (str "Reason: " (parse-json (:body r)))))
      (finally
        (repl/stop)))))

(deftest upload-12MB-file-with-IOException-on-start_partial
  (let [system (repl/start {:filestore
                            (OutputStreamsFailWhenCalledFrom. (called-from? "start_partial"))})]
    (try     
      (comment "This is what should happen with a larger than 1 chunk file"
               (let [r (post-file uid
                                  "./test-resources/metadata-12MB-valid.csv")]
                 (is (= 500
                        (:status r))
                     (str "Reason: " (parse-json (:body r))))))
      (comment "This is what currently occurs:")
      (is (thrown? java.net.SocketException
                   (post-file uid
                              "./test-resources/metadata-12MB-valid.csv")))
      (finally
        (repl/stop)))))


(deftest upload-12MB-file-with-IOException-on-continue
  (let [system (repl/start {:filestore
                            (OutputStreamsFailWhenCalledFrom. (called-from? "continue"))})]
    (try      
      (comment "This is what should happen with a larger than 1 chunk file"
               (let [r (post-file uid
                                  "./test-resources/metadata-12MB-valid.csv")]
                 (is (= 500
                        (:status r))
                     (str "Reason: " (parse-json (:body r))))))
      (comment "This is what currently occurs:")
      (is (thrown? java.net.SocketException
                   (prn (post-file uid
                                   "./test-resources/metadata-12MB-valid.csv"))))
      (finally
        (repl/stop)))))

(deftest upload-12MB-file-with-IOException-on-complete
  (let [system (repl/start {:filestore
                            (OutputStreamsFailWhenCalledFrom. (called-from? "complete"))})]
    (try
      (let [r (post-file uid
                         "./test-resources/metadata-12MB-valid.csv")]
        (is (= 500
               (:status r))
            (str "Reason: " (parse-json (:body r)))))
      (finally
        (repl/stop)))))

(deftest upload-12MB-file-with-IOException-from-completion-channel
  (let [system (repl/start {:filestore
                            (map->ExceptionInChan {})})]
    (try
      (let [r (post-file uid
                         "./test-resources/metadata-12MB-valid.csv")]
        (is (= 500
               (:status r))
            (str "Reason: " (parse-json (:body r)))))
      (finally
        (repl/stop)))))
