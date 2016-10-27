(ns kixi.datastore.schemastore.validator
  (:require [clojure.spec :as s]
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

(defmethod resolve-schema "id"
  [definition schemastore]
  (->
   definition
   (get-in [::ss/schema ::ss/id])
   ((partial ss/fetch-spec schemastore))
   (resolve-schema schemastore)))

(defmethod resolve-schema "boolean"
  [_ _]
  conformers/bool?)

(defn resolve-form
  [definition schemastore]
  (if-let [id (::ss/id definition)]
    (ss/fetch-spec schemastore id)
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
  (let [schema (resolve-schema (ss/fetch-spec schemastore schema-id) schemastore)]
    (s/valid? schema data)))

(defn schema-id->schema
  [schemastore schema-id]
  (resolve-schema (ss/fetch-spec schemastore schema-id) schemastore))

(defn explain-data
  ([schemastore schema-id data]
   (explain-data (resolve-schema (ss/fetch-spec schemastore schema-id) schemastore)
                 data))
  ([schema data]
   (s/explain-data schema data)))
