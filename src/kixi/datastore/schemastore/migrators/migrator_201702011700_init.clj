(ns kixi.datastore.schemastore.migrators.migrator-201702011700-init
  (:require [clojurewerkz.elastisch.rest.index :as esi]
            [kixi.datastore
             [elasticsearch :as es :refer [string-analyzed
                                           string-stored-not_analyzed]]
             [schemastore :as ss]
             [segmentation :as seg]]
            [kixi.datastore.schemastore.elasticsearch
             :refer
             [doc-type index-name]]
            [joplin.elasticsearch.database :refer [client]]))

(def doc-def
  {::ss/id string-stored-not_analyzed
   ::ss/name string-analyzed
   ::ss/provenance {:properties {:kixi.user/id string-stored-not_analyzed                                 
                                 ::ss/created es/timestamp}}
   ::ss/schema {:properties {::ss/tag string-stored-not_analyzed
                             ::ss/type string-stored-not_analyzed
                             ::ss/id string-stored-not_analyzed
                             ::ss/min es/double
                             ::ss/max es/double
                             ::ss/pattern string-stored-not_analyzed
                             ::ss/elements string-stored-not_analyzed
                             ::ss/definition {:type "nested"
                                              :properties {::ss/type string-stored-not_analyzed
                                                           ::ss/id string-stored-not_analyzed
                                                           ::ss/min es/double
                                                           ::ss/max es/double
                                                           ::ss/pattern string-stored-not_analyzed
                                                           ::ss/elements string-stored-not_analyzed}}}}})

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
