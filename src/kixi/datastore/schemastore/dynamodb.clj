(ns kixi.datastore.schemastore.dynamodb
  (:require [clojure.spec :as s]
            [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore
             [dynamodb :as db :refer [migrate]]
             [schema-creator :as sc]
             [schemastore :as ss :refer [SchemaStore]]
             [time :as time]]
            [taoensso.timbre :as timbre :refer [error info]]))

(def dynamodb-client-kws
  #{:endpoint})

(defn primary-schemastore-table
  [profile]
  (str profile "-kixi.datastore-schemastore"))

(def id-col (db/dynamo-col ::ss/id))

(def all-sharing-columns
  (mapv
   #(db/dynamo-col [::ss/sharing %])
   ss/activities))

(defn inject-tag
  [definition]
  (reduce
   (fn [acc [tag d]]
     (conj acc
           (assoc d
                  ::ss/tag tag)))
   []
   (partition 2 definition)))

(defn inject-tags
  [schema]
  (if (get-in schema [::ss/schema ::ss/definition])
    (update-in schema
               [::ss/schema ::ss/definition]
               inject-tag)
    schema))

(defn persist-new-schema
  [merge-data]
  (fn [schema]
    (let [id (::ss/id schema)
          schema' (assoc-in schema [::ss/provenance ::ss/created] (time/timestamp))]
      (if (s/valid? ::ss/stored-schema schema')
        (merge-data id 
                    (dissoc (inject-tags schema') ::ss/id))
        (error "Tried to persist schema but it was invalid:" schema' (s/explain-data ::ss/stored-schema schema'))))))

(defn extract-tag
  [stored-def]
  (mapcat
   (fn [d]
     [(::ss/tag d)
      (dissoc d
              ::ss/tag)])
   stored-def))

(defn extract-tags
  [stored-schema]
  (if (get-in stored-schema [::ss/schema ::ss/definition])
    (update-in stored-schema
               [::ss/schema ::ss/definition]
               extract-tag)
    stored-schema))

(defn response-event
  [r]
  nil)

(defrecord DynamoDb
    [communications profile endpoint
     client get-item]
    SchemaStore
    (authorised
      [_ action id user-groups]
      (when-let [sharing (::ss/sharing (get-item id {:projection all-sharing-columns}))]
        (not-empty (clojure.set/intersection (set (get sharing action))
                                             (set user-groups)))))
    (exists [_ id]
      (not-empty (get-item id {:projection [id-col]})))
    (fetch-with [_ sub-spec]
      (prn "fetching: " sub-spec)
                                        ;      (fetch-with-sub-spec data sub-spec)
      )
    (retrieve [_ id]
      (extract-tags
       (get-item id)))
    component/Lifecycle
    (start [component]
      (if-not client
        (let [client (assoc (select-keys component
                                         dynamodb-client-kws)
                            :profile profile)              
              joplin-conf {:migrators {:migrator "joplin/kixi/datastore/schemastore/migrators/dynamodb"}
                           :databases {:dynamodb (merge
                                                  {:type :dynamo
                                                   :migration-table (str profile "-kixi.datastore-schemastore.migrations")}
                                                  client)}
                           :environments {:env [{:db :dynamodb :migrator :migrator}]}}]
          (info "Starting Schema DynamoDb Store")
          (migrate :env joplin-conf)
          (c/attach-event-handler! communications
                                   :kixi.datastore/schemastore
                                   :kixi.datastore.schema/created
                                   "1.0.0"
                                   (comp response-event (persist-new-schema 
                                                         (partial db/merge-data client (primary-schemastore-table profile) id-col))
                                         :kixi.comms.event/payload))
          (sc/attach-command-handler communications)
          (-> component
              (assoc :client client)
              (assoc :get-item (partial db/get-item client (primary-schemastore-table profile) id-col))))
        component))
    (stop [component]
      (if client
        (do (info "Destroying Schema DynamoDb Store")
            (-> component
                (dissoc :client)
                (dissoc :get-item)))
        component)))
