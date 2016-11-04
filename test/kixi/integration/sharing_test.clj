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

(def shares->authorised-actions
  {[[:file :sharing :file-read]] [get-file]
   [[:file :sharing :meta-visible]] []
   [[:file :sharing :meta-read]] [get-metadata]
   [[:file :sharing :meta-update]] []
   ;[[:sharing :read]] []
   ;[[:sharing :use]] []
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
  [type ugroup shares dload-ugroup]
  (->> shares
       (reduce
        (fn [acc [stype share-area share-specific]]
          (if (= type stype)
            (merge-with (partial merge-with concat)
                        acc
                        {share-area {share-specific [dload-ugroup]}})
            acc))
        {:sharing {:file-read [ugroup]
                   :meta-read [ugroup]}})
       seq
       flatten))

(def shares->file-sharing-map 
  (partial shares->sharing-map :file))

(deftest explore-sharing-level->actions
  (let [post-file (partial base/post-file-and-wait
                           :file-name "./test-resources/metadata-one-valid.csv")
        post-spec (partial base/post-spec-and-wait metadata-file-schema)]
    (doseq [shares (subsets all-shares)]
      (let [upload-uid (uuid)
            upload-ugroup (uuid)
            use-uid (uuid)
            use-ugroup (uuid)
            psr (post-spec upload-uid upload-ugroup)]
        (when-accepted psr
          (let [schema-id (extract-id psr)
                pfr (apply post-file
                           :schema-id schema-id
                           :user-id upload-uid
                           :user-groups upload-ugroup
                           (shares->file-sharing-map upload-ugroup shares use-ugroup))]
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
                      (str "Is " action " NOT allowed with " shares)))))))))))
