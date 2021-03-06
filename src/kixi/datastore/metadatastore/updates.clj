(ns kixi.datastore.metadatastore.updates
  (:require [clojure.spec.alpha :as s]
            [com.rpl.specter :as sp]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.schemastore.conformers :as sc]
            [kixi.datastore.metadatastore
             [geography :as geo]
             [license :as l]
             [time :as mdt]]
            [kixi.datastore.metadatastore :as ms]))

(defn update-spec-name
  [spec]
  (keyword (str (namespace spec) ".update") (name spec)))

(defn update-spec
  [[spec actions]]
  (let [rmless-actions (disj actions :rm)]
    (eval
     `(s/def ~(update-spec-name spec)
        ~(if (:rm actions)
           (s/or :r #{:rm}
                 :o (s/map-of rmless-actions
                              spec))
           (s/map-of rmless-actions
                     spec))))))

(defn update-map-spec
  [[map-spec fields]]
  (eval
   `(s/def ~(update-spec-name map-spec)
      (s/and (s/keys
              :opt ~(mapv update-spec-name fields))
             (s/every-kv ~(into #{} (mapv update-spec-name fields))                         
                         (constantly true))))))

(defn all-specs-with-actions
  [definition-map]
  (distinct
   (sp/select
    [sp/MAP-VALS sp/ALL (sp/if-path [sp/LAST map?]
                                    [sp/LAST sp/ALL]
                                    identity)]
    definition-map)))

(defn sub-maps-with-keys
  [definition-map]
  (distinct
   (mapv (fn [[k v]]
           [k (keys v)])
         (sp/select
          [sp/MAP-VALS sp/ALL (sp/selected? sp/LAST map?)]
          definition-map))))

(defn create-update-specs
  [definition-map]
  (let [update-specs (map update-spec (all-specs-with-actions definition-map))
        map-specs (map update-map-spec (sub-maps-with-keys definition-map))]
    (doall (concat update-specs map-specs))))
