(ns kixi.integration.sharing-test
  (:require [clojure.test :refer :all]
            [clojure.math.combinatorics :as combo :refer [subsets]]
            [environ.core :refer [env]]
            [kixi.integration.base :refer [uuid extract-id when-accepted when-created] :as base]))

(def metadata-file-schema {:name ::metadata-file-schema
                           :schema {:type "list"
                                    :definition [:cola {:type "integer"}
                                                 :colb {:type "integer"}]}})

(use-fixtures :once base/cycle-system-fixture)

(defn authorised
  [resp]
  (#{200 201}
   (:status resp)))

(defn unauthorised 
  [resp]
  (= 401
     (:status resp)))

(defn get-file
  [schema-id file-id uid ugroups]
  (base/get-file file-id uid ugroups))

(defn get-metadata
  [schema-id file-id uid ugroups]
  (base/get-metadata file-id ugroups))

(defn get-spec
  [schema-id file-id uid ugroups]
  (base/get-spec-direct schema-id ugroups))

(defn post-file-using-schema
  [schema-id file-id uid ugroups]
  (base/post-file :file-name "./test-resources/metadata-one-valid.csv"
                  :schema-id schema-id
                  :user-id uid
                  :user-groups ugroups
                  :sharing {:file-read [ugroups]
                            :meta-read [ugroups]}))

(def shares->authorised-actions
  {[[:file :sharing :file-read]] [get-file]
   [[:file :sharing :meta-visible]] []
   [[:file :sharing :meta-read]] [get-metadata]
   [[:file :sharing :meta-update]] []
   [[:schema :sharing :read]] [get-spec]
;   [[:schema :sharing :use]] [post-file-using-schema]
   })

(def all-shares 
  (vec (reduce (partial apply conj) #{} (keys shares->authorised-actions))))

(def all-actions
  (vec (reduce (partial apply conj) #{} (vals shares->authorised-actions))))

(defn actions-for
  [shares]
  (mapcat 
   #(get shares->authorised-actions %)
   (subsets shares)))

(defn shares->sharing-map
  [type shares upload-ugroup use-ugroup]
  (->> shares
       (reduce
        (fn [acc [stype share-area share-specific]]
          (if (= type stype)
            (merge-with concat
                        acc
                        {share-specific [use-ugroup]})
            acc))
        (case type
          :file {:file-read [upload-ugroup]
                 :meta-read [upload-ugroup]}
          :schema {:read [upload-ugroup]
                   :use [upload-ugroup]}))))

(def shares->file-sharing-map 
  (partial shares->sharing-map :file))

(def shares->schema-sharing-map 
  (partial shares->sharing-map :schema))

(defn post-file
  [schema-id file-id upload-uid upload-ugroup use-ugroup shares]
  (base/post-file-and-wait
   :file-name "./test-resources/metadata-one-valid.csv"
   :schema-id schema-id
   :user-id upload-uid
   :user-groups upload-ugroup
   :sharing (shares->file-sharing-map shares upload-ugroup use-ugroup)))

(defn post-spec 
  [shares upload-uid upload-ugroup use-ugroup]
  (base/post-spec-and-wait metadata-file-schema
                           upload-uid
                           upload-ugroup
                           {:sharing (shares->schema-sharing-map
                                      shares
                                      upload-ugroup
                                      use-ugroup)}))

(deftest explore-sharing-level->actions
  (doseq [shares (subsets all-shares)]
    (let [upload-uid (uuid)
          upload-ugroup (uuid)
          use-uid (uuid)
          use-ugroup (uuid)
          psr (post-spec shares upload-uid upload-ugroup use-ugroup)]
      (when-accepted psr
        (let [schema-id (extract-id psr)
              pfr (post-file schema-id nil upload-uid upload-ugroup use-ugroup shares)]
          (when-created pfr
            (let [file-id (extract-id pfr)
                  authorised-actions (actions-for shares)
                  unauthorised-actions (apply disj (set all-actions) authorised-actions)]
              (doseq [action authorised-actions]
                (is (authorised
                     (action
                      schema-id file-id use-uid use-ugroup))
                    (str "Is " action " allowed with " shares)))
              (doseq [action unauthorised-actions]
                (is (unauthorised
                     (action
                      schema-id file-id use-uid use-ugroup))
                    (str "Is " action " NOT allowed with " shares))))))))))
