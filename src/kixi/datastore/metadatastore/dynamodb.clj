(ns kixi.datastore.metadatastore.dynamodb
  (:require [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore
             [communication-specs :as cs]
             [dynamodb :as db :refer [migrate]]
             [metadatastore :as md :refer [MetaDataStore]]]
            [taoensso
             [encore :refer [get-subvector]]
             [timbre :as timbre :refer [info error]]]))

(def dynamodb-client-kws
  #{:endpoint})

(def id-col (db/dynamo-col ::md/id))

(def all-sharing-columns
  (mapv
   #(db/dynamo-col [::md/sharing %])
   md/activities))

(defn primary-metadata-table
  [profile]
  (str profile "-kixi.datastore-metadatastore"))

(defn activity-metadata-table
  [profile]
  (str profile "-kixi.datastore-metadatastore.activity"))

(defn activity-metadata-created-index 
  []
  "provenance-created")

(def activity-table-pk :groupid-activity)

(defn activity-table-id
  [group-id activity]
  (str group-id "_" activity))

(def activity-table-projection
  #{::md/id ::md/provenance ::md/schema ::md/file-type ::md/name ::md/description})

(defn sharing-columns->sets
  [md]
  (reduce
   (fn [acc a]
     (if (get-in acc [::md/sharing a])
       (update-in acc [::md/sharing a] set)
       acc))
   md
   md/activities))

(defmulti update-metadata-processor
  (fn [conn update-event]
    (::cs/file-metadata-update-type update-event)))

(defn insert-activity-row
  [conn group-id activity metadata]
  (let [id (activity-table-id group-id activity)
        projection (assoc
                    (select-keys metadata activity-table-projection)
                    activity-table-pk id)]
    (try
      (db/put-item conn
                   (activity-metadata-table (:profile conn))
                   projection)
      (catch Exception e
        (error e "Failed to insert activity row: " id projection)))))

(defn remove-activity-row
  [conn group-id activity md-id]
  (let [id (activity-table-id group-id activity)]
    (try  
      (db/delete-item conn
                      (activity-metadata-table (:profile conn))
                      {activity-table-pk id
                       id-col md-id})
      (catch Exception e
        (error e "Failed to delete activity row: " id)))))

(defmethod update-metadata-processor ::cs/file-metadata-created
  [conn update-event]
  (let [metadata (::md/file-metadata update-event)]
    (info "Create: " metadata)
    (db/merge-data conn
                   (primary-metadata-table (:profile conn))
                   id-col
                   (::md/id metadata)
                   (-> metadata
                       sharing-columns->sets
                       (dissoc ::md/id)))
    (doseq [activity (keys (::md/sharing metadata))]
      (doseq [group-id (get-in metadata [::md/sharing activity])]
        (insert-activity-row conn group-id activity metadata)))))

(defmethod update-metadata-processor ::cs/file-metadata-structural-validation-checked
  [conn update-event]
  (info "Update: " update-event)
  (db/merge-data conn
                 (primary-metadata-table (:profile conn))
                 id-col
                 (::md/id update-event)
                 (select-keys update-event
                              [::md/structural-validation])))

(defmethod update-metadata-processor ::cs/file-metadata-segmentation-add
  [conn update-event]
  (info "Update: " update-event)
  (db/append-list conn
                  (primary-metadata-table (:profile conn))
                  id-col
                  (::md/id update-event)
                  :add
                  [::md/segmentations]
                  (:kixi.group/id update-event)))

(defmethod update-metadata-processor ::cs/file-metadata-sharing-updated
  [conn update-event]
  (info "Update Share: " update-event)
  (let [update-fn (case (::md/sharing-update update-event)
                    ::md/sharing-conj :add
                    ::md/sharing-disj :delete)
        metadata-id (::md/id update-event)]
    (db/update-set conn
                   (primary-metadata-table (:profile conn))
                   id-col
                   metadata-id
                   update-fn
                   [::md/sharing (::md/activity update-event)]
                   (:kixi.group/id update-event))
    (case (::md/sharing-update update-event)
      ::md/sharing-conj (let [metadata (db/get-item conn
                                                    (primary-metadata-table (:profile conn))
                                                    id-col
                                                    metadata-id)]
                          (insert-activity-row conn (:kixi.group/id update-event) (::md/activity update-event) metadata))
      ::md/sharing-disj (remove-activity-row conn (:kixi.group/id update-event) (::md/activity update-event) metadata-id))))

(defmethod update-metadata-processor ::cs/file-metadata-update
  [conn update-event]
  (info "Update: " update-event)
  (db/merge-data conn
                 (primary-metadata-table (:profile conn))
                 id-col
                 (::md/id update-event)
                 (dissoc update-event
                         ::md/id)))


(def sort-order->dynamo-comp
  {"asc" :asc
   "desc" :desc})

(defn comp-id-created-maps
  [a b]
  (cond
    (nil? a) -1
    (nil? b) 1
    :else (let [c-comp (.compareTo ^String (get-in a [::md/provenance ::md/created])
                                   ^String (get-in b [::md/provenance ::md/created]))]
            (if (zero? c-comp)
              (.compareTo ^String (get a ::md/id)
                          ^String (get b ::md/id))
              c-comp))))

(def sort-order->knit-comp
  {"asc" comp-id-created-maps
   "desc" (fn [a b]
            (comp-id-created-maps b a))})

(defn keep-if-at-least
  "Collection must be ordered"
  [cnt coll]
  (letfn [(enough? [acc] (= cnt acc))
          (process [remaining acc candidate]
            (if (enough? acc)
              (cons candidate
                    (let [remain (drop-while #{candidate} remaining)
                          new-candidate (first remain)]
                      (when new-candidate
                        (lazy-seq
                         (process (rest remain) 1 new-candidate)))))
              (when-let [t (first remaining)]                   
                (if (= t candidate)
                  (recur (rest remaining) (inc acc) candidate)
                  (recur (rest remaining) 1 t)))))]
    (when (first coll)
      (process (rest coll) 1 (first coll)))))

(def enforce-and-sematics keep-if-at-least)

(defn knit-ordered-data
  [comparitor seqs-l]
  (letfn [(head-element [firsts]
            (first
             (sort-by second comparitor
                      (map-indexed (fn [dex v] (vector dex v))
                                   firsts))))
          (more-data [seqs]
            (some identity (map first seqs)))
          (process [seqs]
            (when (more-data seqs) 
              (let [firsts (mapv first seqs)
                    [head-dex head-val] (head-element firsts)]
                (cons head-val
                      (lazy-seq
                       (process (update seqs head-dex rest)))))))]
    (process (vec seqs-l))))

(defn all-ids-ordered 
  [client group-ids activities sort-cols sort-order]
  (->> (for [g group-ids
             a activities]
         (db/query-index client
                         (activity-metadata-table (:profile client))
                         (activity-metadata-created-index) 
                         {activity-table-pk
                          (activity-table-id g a)}
                         [sort-cols ::md/id]
                         (get sort-order->dynamo-comp sort-order)))
       (knit-ordered-data (get sort-order->knit-comp sort-order))
       (mapv ::md/id)
       (enforce-and-sematics (count activities))
       vec))

(defn criteria->activities
  [criteria]
  (->> criteria
       ::md/activities
       (cons ::md/meta-read)
       set))

(defn response-event
  [r]
  nil)

(defrecord DynamoDb
    [communications profile endpoint cluster discover
     client get-item]
    MetaDataStore
    (authorised
      [this action id user-groups]
      (when-let [item (get-item id {:projection all-sharing-columns})]
        (not-empty (clojure.set/intersection (set (get-in item [::md/sharing action]))
                                             (set user-groups)))))
    (exists [this id]
      (get-item id {:projection [id-col]}))
    (retrieve [this id]
      (get-item id))
    (query [this criteria from-index cnt sort-cols sort-order]
      (when-not (= [::md/provenance ::md/created]
                   sort-cols)
        (throw (new Exception "Only created timestamp sort supported")))
      (let [group-ids (:kixi.user/groups criteria)
            activities (criteria->activities criteria)
            all-ids-ordered (all-ids-ordered client group-ids activities sort-cols sort-order)
            target-ids (get-subvector all-ids-ordered from-index cnt)]
        {:items (if (not-empty target-ids)
                  (db/get-bulk-ordered client
                                       (primary-metadata-table profile)
                                       id-col
                                       target-ids)
                  [])
         :paging {:total (count all-ids-ordered)
                  :count (count target-ids)
                  :index from-index}}))

    component/Lifecycle
    (start [component]
      (if-not client
        (let [client (assoc (select-keys component
                                         dynamodb-client-kws)
                            :profile profile)              
              joplin-conf {:migrators {:migrator "joplin/kixi/datastore/metadatastore/migrators/dynamodb"}
                           :databases {:dynamodb (merge
                                                  {:type :dynamo
                                                   :migration-table (str profile "-kixi.datastore-metadatastore.migrations")}
                                                  client)}
                           :environments {:env [{:db :dynamodb :migrator :migrator}]}}]
          (info "Starting File Metadata DynamoDb Store")
          (migrate :env joplin-conf)
          (c/attach-event-handler! communications
                                   :kixi.datastore/metadatastore
                                   :kixi.datastore.file-metadata/updated
                                   "1.0.0"
                                   (comp response-event (partial update-metadata-processor client) :kixi.comms.event/payload))
          (assoc component 
                 :client client
                 :get-item (partial db/get-item client (primary-metadata-table profile) id-col)))
        component))
    (stop [component]
      (if client
        (do (info "Destroying File Metadata DynamoDb Store")
            (dissoc component :client :get-item))
        component)))
