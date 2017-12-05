(ns kixi.acceptance.failure-injection-test
  {:acceptance true}
  (:require [clojure.spec.test.alpha :refer [with-instrument-disabled]]
            [clojure.test :refer :all]
            [cheshire.core :as json]
            [kixi.datastore
             [metadatastore :as ms]
             [schemastore :as ss]]
            [kixi.integration.base :as base :refer :all]
            [kixi.log :as kixi-log]
            [taoensso.timbre :as log]))


(defn force-logging-to-json
  [all-tests]
  (let [current-config log/*config*]
    (try
      (log/set-config!
       (assoc current-config
              :appenders
              {:direct-json (kixi-log/timbre-appender-logstash)}))
      (all-tests)
      (finally
        (log/set-config! current-config)))))

(use-fixtures :once
  cycle-system-fixture
  extract-comms
  force-logging-to-json)

(defmacro alter-var-root-wrap
  [v f & body]
  `(let [original-binding# (var-get ~v)]
     (try
       (alter-var-root ~v (constantly ~f))
       ~@body
       (finally
         (alter-var-root ~v (constantly original-binding#))))))

(deftest metadata-dynamo-fetch-failure
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata
                            uid
                            "./test-resources/metadata-one-valid.csv"))]
    (when-success metadata-response
      (let [meta-id (extract-id metadata-response)
            metadata-response (get-metadata uid meta-id)]
        (is-submap {:status 200}
                   metadata-response)
        (alter-var-root-wrap
         #'kixi.datastore.dynamodb/get-item
         (fn [& args] (throw (new com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
                                  "Surprise")))
         (let [metadata-error-response (get-metadata uid meta-id)]
           (is-submap {:status 500
                                        ;the body is transit encoded json, but can't access the decoded easily
                       :body  "[\"^ \",\"~:msg\",\"Server Error, see logs\"]"}
                      metadata-error-response)))))))
