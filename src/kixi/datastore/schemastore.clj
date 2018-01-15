(ns kixi.datastore.schemastore
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [kixi.datastore.schemastore.conformers
             :as
             sc
             :refer
             [ns-keyword timestamp uuid]]))

(s/def ::id uuid)
(s/def ::tag keyword?)
(s/def ::created timestamp)
(s/def ::name ns-keyword)
(s/def ::min number?)
(s/def ::max number?)
(s/def ::pattern string?)
(s/def ::elements (s/with-gen vector?
                    #(gen/vector (gen/string)
                              1
                              10)))
(s/def ::source #{"upload"})


(defmulti provenance-type ::source)

(defmethod provenance-type "upload"
  [_]
  (s/keys :req [::source :kixi.user/id]
          :opt [::created]))

(s/def ::provenance (s/multi-spec provenance-type ::source))

(def activities
  [::read ::use])

(s/def ::sharing
  (s/map-of (set activities)
            (s/with-gen
              (s/coll-of :kixi.group/id)
              #(gen/vector (s/gen uuid) 1 10))))

(s/def ::definition
  (s/cat :pairs (s/+ (s/cat :tag ::tag
                            :type ::primitive-schema))))

(s/def ::list-spec
  (s/keys :req [::type ::definition]))

(defmulti schema-type ::type)

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

(s/def ::primitive-schema
  (s/multi-spec schema-type ::type))

(s/def ::schema
  (s/or
   :list ::list-spec
   :primitive ::primitive-schema))

(s/def ::type (set (conj (keys (methods schema-type))
                         "list")))

(s/def ::stored-schema
  (s/keys :req [::schema ::id ::name ::provenance ::sharing]))

(s/def ::create-schema-request
  (s/keys :req [::schema ::id ::name ::sharing]))

(defprotocol SchemaStore
  (authorised
    [this action id user-groups])
  (exists [this spec-name])
  (fetch-with [this sub-schema])
  (retrieve [this id]))
