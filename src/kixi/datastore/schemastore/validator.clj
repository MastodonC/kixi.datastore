(ns kixi.datastore.schemastore.validator
  (:require [clojure.spec.alpha :as s]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]))

(defn invalid-name?
  [name']
  (s/explain-data ::ss/name name'))

(defn invalid-schema?
  [schema]
  (s/explain-data ::ss/schema schema))

(defmulti resolve-schema
  (fn [schema _]
    (get-in schema [::ss/schema ::ss/type])))

(defmethod resolve-schema "integer"
  [_ _]
  conformers/integer?)

(defmethod resolve-schema "integer-range"
  [definition _]
  (let [{min ::ss/min max ::ss/max} (::ss/schema definition)]
    (conformers/integer-range? min max)))

(defmethod resolve-schema "double"
  [_ _]
  conformers/double?)

(defmethod resolve-schema "double-range"
  [definition _]
  (let [{min ::ss/min max ::ss/max} (::ss/schema definition)]
    (conformers/double-range? min max)))

(defmethod resolve-schema "id"
  [definition schemastore]
  (->
   definition
   (get-in [::ss/schema ::ss/id])
   ((partial ss/retrieve schemastore))
   (resolve-schema schemastore)))

(defmethod resolve-schema "set"
  [definition _]
  (let [{elements ::ss/elements} (::ss/schema definition)]
    (apply conformers/set? elements)))

(defmethod resolve-schema "boolean"
  [_ _]
  conformers/bool?)

(defmethod resolve-schema "pattern"
  [definition _]
  (conformers/regex? (get-in definition [::ss/schema ::ss/pattern])))

(defmethod resolve-schema "string"
  [_ _]
  conformers/-string?)

(defn resolve-form
  [definition schemastore]
  (if-let [id (::ss/id definition)]
    (ss/retrieve schemastore id)
    definition))

(defmethod resolve-schema "list"
  [definition schemastore]
  (let [schema-def (get-in definition [::ss/schema ::ss/definition])]
    (s/def-impl (keyword "kixi.schemas" (::ss/id definition))
      schema-def
      (s/tuple-impl (take-nth 2 (rest schema-def))
                    (mapv
                     (fn [x] (resolve-schema {::ss/schema x} schemastore))
                     (take-nth 2 (rest schema-def))))))
  (keyword "kixi.schemas" (::ss/id definition)))

(defn valid?
  [schemastore schema-id data]
  (let [schema (resolve-schema (ss/retrieve schemastore schema-id) schemastore)]
    (s/valid? schema data)))

(defn schema-id->schema
  [schemastore schema-id]
  (when-let [s (ss/retrieve schemastore schema-id)]
    (resolve-schema s schemastore)))

(defn explain-data
  ([schemastore schema-id data]
   (explain-data (resolve-schema (ss/retrieve schemastore schema-id) schemastore)
                 data))
  ([schema data]
   (s/explain-data schema data)))
