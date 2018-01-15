(ns kixi.datastore.patches.patch-20180111
  (:require [kixi.comms :as comms]
            [kixi.datastore.dynamodb :as db]
            [kixi.datastore.metadatastore.dynamodb :as mdb]
            [kixi.datastore.metadatastore :as ms]
            [kixi.datastore.kaylee :as k]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Patch -
;; Date: 11/01/2018
;; Created by: AW, TC, JB
;;
;; This patch will update the event stream and add the user id of the user
;; executing the patch. It will scan the table and add the bundle add permission
;; to users with metaupdate on a bundle. The patch must be run manually.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- db
  []
  (:client (:metadatastore @kixi.datastore.application/system)))

(defn- comms
  []
  (:communications @kixi.datastore.application/system))

(defn- profile
  []
  (name @kixi.datastore.application/profile))

(defn- get-bundles
  ([]
   (get-bundles 100))
  ([n]
   (db/slow-scan (db) (mdb/primary-metadata-table (profile)) (keyword mdb/id-col)
                 {:limit n :verbose? true
                  :filter-expr "#t = :t"
                  :expr-attr-vals {":t" "bundle"}
                  :expr-attr-names {"#t" (db/dynamo-col ::ms/type)}})))

(defn- grant-bundle-add!
  [user-id bundle]
  (let [id (get bundle (db/dynamo-col-k ::ms/id))
        mu (get bundle (db/dynamo-col-k [::ms/sharing ::ms/meta-update]))]
    (run! (partial k/send-sharing-update user-id id ::ms/sharing-conj ::ms/bundle-add) mu)))

(defn apply-patch!
  [your-user-id]
  (println "This could take some time.")
  (let [bundles (get-bundles)
        _ (println "Finished getting bundles")]
    (run! (partial grant-bundle-add! your-user-id) bundles)))
