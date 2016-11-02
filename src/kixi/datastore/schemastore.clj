(ns kixi.datastore.schemastore
  (:require [clojure.spec :as s]
            [kixi.datastore.schemastore.conformers :as sc :refer [uuid timestamp?]]))

(s/def ::id uuid)
(s/def ::tag keyword?)
(s/def ::timestamp timestamp?)
(s/def ::name #(and (keyword? %)
                    (namespace %)))

(def activities
  [::read ::use])

(s/def ::sharing
  (s/map-of (set activities)
            (s/coll-of :kixi.user-group/id)))

(s/def ::definition
  (s/cat :pairs (s/+ (s/cat :tag ::tag
                            :type ::schema))))

(defmulti schema-type ::type)

(defmethod schema-type "list" [_]
  (s/keys :req [::type ::definition]))

(defmethod schema-type "integer" [_]
  (s/keys :req [::type]))

(defmethod schema-type "double" [_]
  (s/keys :req [::type]))

(defmethod schema-type "id" [_]
  (s/keys :req [::type ::id]))

(defmethod schema-type "integer-range" [_]
  (s/keys :req [::type ::min ::max]))

(defmethod schema-type "double-range" [_]
  (s/keys :req [::type ::min ::max]))

(defmethod schema-type "set" [_]
  (s/keys :req [::type ::elements]))

(defmethod schema-type "boolean" [_]
  (s/keys :req [::type]))

(defmethod schema-type "pattern" [_]
  (s/keys :req [::type ::pattern]))

(defmethod schema-type "string" [_]
  (s/keys :req [::type]))

(s/def ::schema
  (s/multi-spec schema-type ::type))

(s/def ::stored-schema
  (s/keys :req [::schema ::id ::name ::timestamp ::sharing]))

(s/def ::create-schema-request
  (s/keys :req [::schema ::id ::name ::sharing]))

(defprotocol SchemaStore
  (exists [this spec-name])
  (fetch-with [this sub-schema])
  (fetch-spec [this spec-name]))
