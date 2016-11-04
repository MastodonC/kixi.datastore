(ns kixi.unit.schemastore.schemastore-test
  (:require [clojure
             [spec :as s]
             [test :refer :all]]
            [clojure.spec.gen :as gen]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [environ.core :refer [env]]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore
             [inmemory :as ssim]
             [validator :as sv]]
            [kixi.integration.base :refer :all]))

(defonce schemastore (atom nil))

(defn inmemory-schemastore-fixture
  [all-tests]
  (reset! schemastore (ssim/map->InMemory {:data (atom {})}))
  (all-tests)
  (reset! schemastore nil))

(use-fixtures :once inmemory-schemastore-fixture)

(def gen-int (s/gen (s/or :int integer?
                          :str string?)
                    {[:str] #(gen/fmap str (gen/int))}))

(defn gen-int-range 
  "Our int-ranges are inclusive at both ends, hence the inc's"
  [min max]
  (s/gen (s/or :int (s/int-in min (inc max))
               :str string?)
         {[:str] #(gen/fmap str (s/gen (s/int-in min (inc max))))}))


(def gen-double-str
  (gen/fmap str (gen/double)))

(def gen-double (s/gen (s/or :dbl double?
                             :str string?)
                       {[:str] (constantly gen-double-str)}))

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

(def sample-size (Integer/valueOf (str (env :generative-testing-size "100"))))

(defn double-str? 
  [^String s]
  (try
    (Double/valueOf s)
    (catch NumberFormatException e
      false)))

(defmacro test-schema
  [schema-name schema-def good-generator bad-generator]
  `(let [id#         (uuid)
         schema-req# {::ss/name ~schema-name
                      ::ss/schema ~schema-def
                      ::ss/id id#
                      ::ss/sharing {}}]
     (ssim/persist-new-schema (:data @schemastore) schema-req#)
     (is-submap schema-req# (ss/fetch-spec @schemastore id#))
     (checking "good data passes" sample-size
               [gd# ~good-generator]
               (is (nil? (sv/explain-data @schemastore id# gd#))))
     (checking "bad data gets explained" sample-size
               [bd# ~bad-generator]
               (is (get (sv/explain-data @schemastore id# bd#) ::s/problems)))))

(deftest integer-list
  (test-schema ::list-one-integer
               {::ss/type "list"
                ::ss/definition [:integer {::ss/type "integer"}]}
               (gen/tuple gen-int)
               (gen/tuple (gen/fmap 
                           #(if double-str?
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
                           #(if double-str?
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
  (let [valid-bool-strs ["true" "TRUE" "t" "T" 
                         "false" "FALSE" "f" "F"]]
    (test-schema ::list-one-bool
                 {::ss/type "list"
                  ::ss/definition [:boolean {::ss/type "boolean"}]}
                 (gen/tuple (gen/one-of [(s/gen boolean?)
                                         (gen/elements valid-bool-strs)]))
                 (gen/tuple (gen/one-of [(s/gen nil?)
                                         (gen/fmap
                                          #(if (or (boolean? %) 
                                                   ((set valid-bool-strs) %))
                                             "x"
                                             %)
                                          (gen/any))])))))

(deftest regex-list
  "Very hard to test regex's, just a dummy example will have to do"
  (test-schema ::list-one-regex
               {::ss/type "list"
                ::ss/definition [:regex {::ss/type "pattern"
                                         ::ss/pattern "-?[0-9]+"}]}
               (gen/tuple (gen/fmap str (s/gen int?)))
               (gen/tuple (gen/char-alpha))))

(deftest string-list
  (test-schema ::list-one-string
               {::ss/type "list"
                ::ss/definition [:string {::ss/type "string"}]}
               (gen/tuple (s/gen string?))
               (s/gen nil?)))
