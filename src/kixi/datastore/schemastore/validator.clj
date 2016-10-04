(ns kixi.datastore.schemastore.validator
  (:require [clojure.spec :as s]))

(def namespace-whitelist
  #{"kixi.datastore.schemastore.conformers"})

(s/def ::valid-symbol
  #(and
    (symbol? %)
    (namespace-whitelist (namespace %))))

(s/def ::valid-schema-name
  #(and (keyword? %)
        (namespace %)))

(s/def ::valid-seq-spec
  (s/cat :cat #{'clojure.spec/cat}
         :pairs (s/+ (s/cat :name keyword?
                            :pred (s/or :name ::valid-schema-name
                                        :symbol ::valid-symbol)) )))

(s/def ::legal-spec
  (s/or :symbol ::valid-symbol
        :name   ::valid-schema-name
        :seq    ::valid-seq-spec))

(defn invalid-name?
  [name']
  (s/explain-data ::valid-schema-name name'))

(defn invalid-definition?
  [form]
  (s/explain-data ::legal-spec form))
