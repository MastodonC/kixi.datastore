(ns kixi.integration.sharing-test
  (:require [clojure.test :refer :all]
            [clojure.math.combinatorics :as combo :refer [subsets]]
            [environ.core :refer [env]]
            [kixi.datastore.schemastore :as ss]
            [kixi.datastore.metadatastore :as ms]
            [kixi.integration.base :refer [uuid extract-id
                                           when-accepted when-created
                                           when-success] :as base]))

(def metadata-file-schema {::ss/name ::metadata-file-schema
                           ::ss/provenance {::ss/source "upload"}
                           ::ss/schema {::ss/type "list"
                                        ::ss/definition [:cola {::ss/type "integer"}
                                                         :colb {::ss/type "integer"}]}})

(use-fixtures :once base/cycle-system-fixture base/extract-comms)

(defn authorised
  [resp]
  (= 200
     (:status resp)))

(defn unauthorised
  [event]
  (let [key (or (:kixi.comms.event/key event)
                (:kixi.event/type event))]
    (if key
      (and
       (.contains (name key) "rejected")
       (= :unauthorised (or (get-in event [:kixi.comms.event/payload :reason])
                            (get event :reason))))
      (= 401 (:status event)))))

(defn get-file
  [schema-id file-id uid ugroups]
  (base/get-file uid ugroups file-id))

(defn get-file-link
  [schema-id file-id uid ugroups]
  (let [event (base/get-dload-link-event uid ugroups file-id)]
    (if (= :kixi.datastore.filestore/download-link-created
           (:kixi.comms.event/key event))
      {:status 200}
      event)))

(defn get-metadata
  [schema-id file-id uid ugroups]
  (base/get-metadata ugroups file-id))

(defn get-spec
  [schema-id file-id uid ugroups]
  (base/get-spec ugroups schema-id))

(defn add-meta-read
  [schema-id file-id uid ugroups]
  (let [event (base/update-metadata-sharing
               uid ugroups
               file-id
               ::ms/sharing-conj
               ::ms/meta-read
               (uuid))]
    (if (= (:kixi.comms.event/key event)
           :kixi.datastore.file-metadata/updated)
      {:status 200}
      event)))

(defn remove-meta-read
  [schema-id file-id uid ugroups]
  (let [event (base/update-metadata-sharing
               uid ugroups
               file-id
               ::ms/sharing-disj
               ::ms/meta-read
               (uuid))]
    (if (= (:kixi.comms.event/key event)
           :kixi.datastore.file-metadata/updated)
      {:status 200}
      event)))

(defn update-metadata
  [schema-id file-id uid ugroups]
  (let [event (base/update-metadata
               uid ugroups
               file-id
               {:kixi.datastore.metadatastore.update/source {:set "Updated source"}})]
    (if (= (:kixi.comms.event/key event)
           :kixi.datastore.file-metadata/updated)
      {:status 200}
      event)))

(defn create-datapack-with
  [schema-id file-id uid ugroups]
  (let [event (base/send-datapack
               uid ugroups
               "sharing test datapack"
               #{file-id})]
    (if (= (:kixi.comms.event/key event)
           :kixi.datastore.file-metadata/updated)
      {:status 200}
      event)))

(defn delete-file
  [schema-id file-id uid ugroups]
  (let [event (base/send-file-delete
               uid ugroups
               file-id)]
    (if (= (:kixi.event/type event)
           :kixi.datastore/file-deleted)
      {:status 200}
      event)))

(defn post-file-using-schema
  [schema-id file-id uid ugroups]
  (base/send-file-and-metadata
   uid ugroups
   (assoc
    (base/create-metadata
     uid
     "./test-resources/metadata-one-valid.csv"
     schema-id)
    ::ms/sharing {::ms/meta-read [ugroups]})))

(def shares->authorised-actions
  {[[:file :sharing ::ms/file-read]] [get-file get-file-link]
   [[:file :sharing ::ms/meta-visible]] []
   [[:file :sharing ::ms/meta-read]] [get-metadata create-datapack-with]
   [[:file :sharing ::ms/meta-update]] [add-meta-read remove-meta-read update-metadata delete-file]
   [[:schema :sharing ::ss/read]] [get-spec]
   [[:schema :sharing ::ss/use]] [post-file-using-schema]})

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
  (reduce
   (fn [acc [stype share-area share-specific]]
     (if (= type stype)
       (merge-with (comp vec concat)
                   acc
                   {share-specific [use-ugroup]})
       acc))
   (case type
     :file {::ms/file-read [upload-ugroup]
            ::ms/meta-read [upload-ugroup]}
     :schema {::ss/read [upload-ugroup]
              ::ss/use [upload-ugroup]})
   shares))

(def shares->file-sharing-map
  (partial shares->sharing-map :file))

(def shares->schema-sharing-map
  (partial shares->sharing-map :schema))

(defn post-file
  [schema-id file-id upload-uid upload-ugroup use-ugroup shares]
  (base/send-file-and-metadata
   upload-uid [upload-ugroup use-ugroup]
   (assoc
    (base/create-metadata
     upload-uid
     "./test-resources/metadata-one-valid.csv"
     schema-id)
    ::ms/sharing (shares->file-sharing-map shares upload-ugroup use-ugroup))))

(defn post-spec
  [shares upload-uid upload-ugroup use-ugroup]
  (base/send-spec upload-uid
                  (assoc-in
                   (assoc metadata-file-schema
                          ::ss/sharing (merge-with (comp vec concat)
                                                   {::ss/read [upload-uid]}
                                                   (shares->schema-sharing-map
                                                    shares
                                                    upload-ugroup
                                                    use-ugroup)))
                   [::ss/provenance :kixi.user/id] upload-uid)))

(deftest explore-sharing-level->actions
  (doseq [shares (subsets all-shares)]
    (let [upload-uid (uuid)
          upload-ugroup (uuid)
          use-uid (uuid)
          use-ugroup (uuid)
          psr (post-spec shares upload-uid upload-ugroup use-ugroup)]
      (when-success psr
        (let [schema-id (::ss/id (base/extract-schema psr))]
          (let [authorised-actions (actions-for shares)
                unauthorised-actions (apply disj (set all-actions) authorised-actions)]
            (doseq [action authorised-actions]
              (let [pfr (post-file schema-id nil upload-uid upload-ugroup use-ugroup shares)]
                (when-success pfr
                  (let [file-id (extract-id pfr)]
                    (is (authorised
                         (action
                          schema-id file-id use-uid use-ugroup))
                        (str "Is " action " allowed with " (into [] shares)))))))
            (doseq [action unauthorised-actions]
              (let [pfr (post-file schema-id nil upload-uid upload-ugroup use-ugroup shares)]
                (when-success pfr
                  (let [file-id (extract-id pfr)]
                    (is (unauthorised
                         (action
                          schema-id file-id use-uid use-ugroup))
                        (str "Is " action " NOT allowed with " (into [] shares)))))))))))))
