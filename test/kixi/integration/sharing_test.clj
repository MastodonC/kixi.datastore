(ns kixi.integration.sharing-test
  (:require [clojure.test :refer :all]
            [clojure.math.combinatorics :as combo :refer [subsets]]
            [environ.core :refer [env]]
            [kixi.integration.base :refer [uuid extract-id] :as base]))

(def metadata-file-schema {:name ::metadata-file-schema
                           :type "list"
                           :definition [:cola {:type "integer"}
                                        :colb {:type "integer"}]})

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
  [schema-id file-id]
  (base/get-file file-id))

(defn get-metadata
  [schema-id file-id]
  (base/get-metadata file-id))

(def shares->authorised-actions
  {[[:file-sharing :read]] [get-file]
   [[:file-metadata-sharing :visible]] []
   [[:file-metadata-sharing :read]] [get-metadata]
   [[:file-metadata-sharing :update]] [get-metadata]})

(def all-shares 
  (vec (reduce (partial apply conj) #{} (keys shares->authorised-actions))))

(def all-actions
  (vec (reduce (partial apply conj) #{} (vals shares->authorised-actions))))

(defn actions-for
  [shares]
  (mapcat 
   #(get shares->authorised-actions %)
   (subsets shares)))

(defn shares->file-shares
  [uid shares]
  (->> shares
       (reduce
        (fn [acc [share-area share-specific]] 
          (merge-with merge
                      acc
                      {share-area {share-specific [uid]}}))
        {})
       seq
       flatten))

(defmacro when-status
  [status resp rest]
  `(let [rs# (:status ~resp)]
     (base/is-submap {:status ~status}
                ~resp)
     (when (= ~status
              rs#)
       ~@rest)))

(defmacro when-accepted
  [resp & rest]
  `(when-status 202 ~resp ~rest))

(defmacro when-created
  [resp & rest]
  `(when-status 201 ~resp ~rest))

(deftest explore-sharing-level->actions
  (let [post-file (partial base/post-file-and-wait
                           :file-name "./test-resources/metadata-one-valid.csv")
        post-spec #(base/post-spec-and-wait metadata-file-schema)] ;will become partial with spec perms
    (doseq [shares (subsets all-shares)]
      (let [uid (uuid)
            psr (post-spec)]
        (when-accepted psr
          (let [schema-id (extract-id psr)
                pfr (apply post-file
                           :schema-id schema-id
                           :user-id uid
                           (shares->file-shares uid shares))]
            (when-created pfr
              (let [file-id (extract-id pfr)
                    authorised-actions (actions-for shares)
                    unauthorised-actions (apply disj (set all-actions) authorised-actions)]
                (doseq [action authorised-actions]
                  (is (authorised
                       (action
                        schema-id file-id))))
                (doseq [action unauthorised-actions]
                  (is (unauthorised
                       (action
                        schema-id file-id))))))))))))
