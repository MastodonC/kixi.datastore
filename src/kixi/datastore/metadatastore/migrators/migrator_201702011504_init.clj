(ns kixi.datastore.metadatastore.migrators.migrator-201702011504-init
  (:require [clojurewerkz.elastisch.rest.index :as esi]
            [kixi.datastore
             [elasticsearch :as es]
             [metadatastore :as ms]
             [schemastore :as ss]
             [segmentation :as seg]]
            [kixi.datastore.metadatastore.elasticsearch
             :refer
             [doc-type index-name]]
            [joplin.elasticsearch.database :refer [client]]))

(def doc-def
  {::ms/id es/string-stored-not_analyzed
   ::ms/type es/string-stored-not_analyzed
   ::ms/name es/string-analyzed
   ::ms/description es/string-analyzed
   ::ms/schema {:properties {::ss/id es/string-stored-not_analyzed
                             ::ms/added es/timestamp}}
   ::ms/provenance {:properties {::ms/source es/string-stored-not_analyzed
                                 :kixi.user/id es/string-stored-not_analyzed
                                 ::ms/parent-id es/string-stored-not_analyzed
                                 ::ms/created es/timestamp}}
   ::ms/segmentation {:type "nested"
                      :properties {::seg/type es/string-stored-not_analyzed
                                   ::seg/line-count es/long
                                   ::seg/value es/string-stored-not_analyzed}}
   ::ms/sharing {:properties (zipmap ms/activities
                                     (repeat es/string-stored-not_analyzed))}})


(defn up
  [db]
  (esi/create (client db)
              index-name
              {:mappings {doc-type 
                          {:properties (es/all-keys->es-format doc-def)}}
               :settings {}}))

(defn down
  [db]
  (esi/delete (client db)
              index-name))
