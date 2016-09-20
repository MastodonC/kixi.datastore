(ns kixi.integration.spec-test
  (:require [clojure.test :refer :all   ;:exclude [deftest]
             ]
            [clj-http.client :as client]
            [kixi.integration.base :refer [service-url cycle-system-fixture uuid]]
            [kixi.datastore.transit :as t]))

(use-fixtures :once cycle-system-fixture)

(def schema-url (str "http://" (service-url) "/schema/"))

(defn post-spec
  [n s]
  (client/post (str schema-url (name n))
               {:form-params {:definition s}
                :content-type :transit+json
                :transit-opts {:encode t/write-handlers
                               :decode t/read-handlers}}))

(defn get-spec
  [n]
  (client/get (str schema-url (name n))
              {:accept :transit+json
               :as :stream
               :throw-exceptions false}))

(defn extract-spec
  [r-g]
  (when (= 200 (:status r-g))
    (-> r-g
        :body
        (client/parse-transit :json {:decode t/read-handlers})
        :definition)))

(deftest unknown-spec-404
  (let [r-g (get-spec :foo)]
    (is (= 404
           (:status r-g)))))

(deftest round-trip-predicate-only-spec
  (let [r-p (post-spec :integer 'integer?)
        r-g (get-spec :integer)]
    (is (= 200
           (:status r-p)))
    (is (= 200
           (:status r-g)))
    (is (= 'integer?
           (extract-spec r-g)))))

