(ns kixi.unit.schemastore.schemastore-test
  (:require [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.validator :as sv]
            [kixi.datastore.schemastore.inmemory :as ssim]
            [kixi.datastore.system :refer [new-system]]
            [kixi.comms :as c]
            [kixi.integration.base :refer :all]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all]
            [clojure.spec.gen :as gen]
            [clojure.spec :as s]))

(defonce system (atom nil))

(defn inmemory-schemastore-fixture
  [all-tests]
  (let [kds (select-keys (new-system :local) [:schemastore :communications])]
    (reset! system (component/start-system kds))
    (all-tests)
    (reset! system (component/stop-system @system))))

(use-fixtures :once inmemory-schemastore-fixture)

(defn wait-for-schema-id
  [id schemastore]
  (loop [tries 40]
    (when-not (ss/fetch-spec schemastore id)
      (Thread/sleep 100)
      (if (zero? (dec tries))
        (throw (Exception. "Schema ID never appeared."))
        (recur (dec tries))))))

(def gen-int (s/gen (s/or :int integer?
                          :str string?)
                    {[:str] #(gen/fmap str (gen/int))}))

(defn gen-int-range 
  "Our int-ranges are inclusive at both ends, hence the inc's"
  [min max]
  (s/gen (s/or :int (s/int-in min (inc max))
               :str string?)
         {[:str] #(gen/fmap str (s/gen (s/int-in min (inc max))))}))

(def gen-double (s/gen (s/or :dbl double?
                             :str string?)
                       {[:str] #(gen/fmap str (gen/double))}))

(defn gen-double-range 
  [& {:keys [min max]}]
  (s/gen (s/or :int (s/double-in :min min :max max)
               :str string?)
         {[:str] #(gen/fmap str (s/gen (s/double-in :min min :max max)))}))

(defn gen-set
  [elements]
  (s/gen (set elements)))

(def gen-bool
  (s/gen (s/or :bool (gen/boolean)
               :str  string?)
         {[:str] #()}))

(def sample-size 100)

(defmacro test-schema
  [schema-name schema-def good-generator bad-generator]
  `(let [{communications# :communications schemastore# :schemastore} (deref system)
         id#         (uuid)
         schema-req# {::ss/name ~schema-name
                      ::ss/schema ~schema-def
                      ::ss/id id#}]
     (c/send-event! communications# :kixi.datastore/schema-created "1.0.0" schema-req#)
     (wait-for-schema-id id# schemastore#)
     (is-submap schema-req# (ss/fetch-spec schemastore# id#))
     (doseq [gd# (gen/sample ~good-generator sample-size)]
       (is (nil? (sv/explain-data schemastore# id# gd#))))
     (doseq [bd# (gen/sample ~bad-generator sample-size)]
       (is (get (sv/explain-data schemastore# id# bd#) ::s/problems)))))

(deftest integer-list
  (test-schema ::list-one-integer
               {::ss/type "list"
                ::ss/definition [:integer {::ss/type "integer"}]}
               (gen/tuple gen-int)
               (gen/tuple (gen/fmap 
                           #(if (re-matches #"[0-9]+" %)
                              (str % "x")
                              %)
                           (s/gen string?)))))

(deftest integer-range-list
  (let [min 3
        max 20]
    (test-schema ::list-one-integer
                 {::ss/type "list"
                  ::ss/definition [:integer-range {::ss/type "integer-range"
                                                   ::ss/min min
                                                   ::ss/max max}]}
                 (gen/tuple (gen-int-range min max))
                 (gen/tuple (s/gen (s/or :lower int?
                                         :higher int?)
                                   {[:lower] #(gen-int-range Integer/MIN_VALUE (dec min))
                                    [:higher] #(gen-int-range (inc max) (dec Integer/MAX_VALUE))})))))

(deftest double-list
  (test-schema ::list-one-double
               {::ss/type "list"
                ::ss/definition [:double {::ss/type "double"}]}
               (gen/tuple gen-double)
               (gen/tuple (gen/fmap 
                           #(if (re-matches #"[0-9]+" %)
                              (str % "x")
                              %)
                           (s/gen string?)))))

(deftest double-range-list
  (let [min -10.3
        max 10.6]
    (test-schema ::list-one-double-range
                 {::ss/type "list"
                  ::ss/definition [:double-range {::ss/type "double-range"
                                                  ::ss/min min
                                                  ::ss/max max}]}
                 (gen/tuple (gen-double-range :min min :max max))
                 (gen/tuple (s/gen nil?)))))
(comment
  "This should be the bad data generator for double-range-list, but it just doesn't work"
  (gen/tuple (s/gen (s/or :lower double?
                          :higher double?)
                    {[:lower] #(gen-double-range :max (+ max 0.1))
                     [:higher] #(gen-double-range :min (- min 0.1))})))

(deftest sets
  (let [elements [1 2 3 4]
        not-in-elements "a"]
    (test-schema ::list-set
                 {::ss/type "list"
                  ::ss/definition [:set {::ss/type "set"
                                         ::ss/elements elements}]}
                 (gen/tuple (gen-set [1 2 3 4]))
                 (gen/tuple (gen/fmap 
                             #(if ((set elements) %)
                                not-in-elements
                                %)
                             (gen/any))))))

(deftest bool-list
  (test-schema ::list-one-bool
               {::ss/type "list"
                ::ss/definition [:boolean {::ss/type "boolean"}]}
               (gen/tuple (s/gen (s/or :bool boolean?
                                       :str string?)))
               (gen/tuple (s/gen nil?))))

(deftest regex-list
  "Very hard to test regex's, just a dummy example will have to do"
  (test-schema ::list-one-regex
               {::ss/type "list"
                ::ss/definition [:regex {::ss/type "pattern"
                                         ::ss/pattern "-?[0-9]+"}]}
               (gen/tuple (gen/fmap str (s/gen int?)))
               (gen/tuple (gen/char-alpha))))
