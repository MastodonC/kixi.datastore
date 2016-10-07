(ns kixi.datastore.schemastore
  (:require [clojure.spec :as s]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [kixi.datastore.schemastore.conformers :as sc]))

(defn timestamp
  []
  (tf/unparse
   (tf/formatters :basic-date-time)
   (t/now)))

(defn timestamp?
  [s]
  (tf/parse
   (tf/formatters :basic-date-time)
   s))

(defn uuid?
  [s]
  (re-find #"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
           s))

(s/def ::id uuid?)
(s/def ::tag keyword?)
(s/def ::timestamp timestamp?)
(s/def ::name #(and (keyword? %)
                    (namespace %)))

(s/def ::definition
  (s/cat :pairs (s/+ (s/cat :tag ::tag
                            :type ::schema))))

(defmulti schema-type ::type)

(defmethod schema-type "list" [_]
  (s/keys :req [::type ::definition]))

(defmethod schema-type "integer" [_]
  (s/keys :req [::type]))

(defmethod schema-type "id" [_]
  (s/keys :req [::type ::id]))

(defmethod schema-type "integer-range" [_]
  (s/keys :req [::type ::min ::max]))

(s/def ::schema
  (s/multi-spec schema-type ::type))

(s/def ::stored-schema
  (s/keys :req [::schema ::id ::name ::timestamp]))

(s/def ::create-schema-request
  (s/keys :req [::schema ::id ::name]))

(defprotocol SchemaStore
  (exists [this spec-name])
  (fetch-with [this sub-schema])
  (fetch-spec [this spec-name]))
