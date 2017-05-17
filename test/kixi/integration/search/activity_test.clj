(ns kixi.integration.search.activity-test
  (:require [clojure.test :refer :all]
            [clojure.math.combinatorics :as combo :refer [subsets]]
            [kixi.datastore
             [metadatastore :as ms]
             [web-server :as ws]]
            [kixi.integration.base :as base :refer :all :exclude [create-metadata]]))

(alias 'ms 'kixi.datastore.metadatastore)

(defn create-metadata
  [uid]
  (base/create-metadata uid "./test-resources/metadata-one-valid.csv"))

(use-fixtures :once
  cycle-system-fixture
  extract-comms)

(deftest novel-user-finds-nothing
  (is-submap {:items []
              :paging {:total 0 
                       :count 0
                       :index 0}}
             (:body
              (search-metadata (uuid) [::ms/file-read]))))

(deftest unknown-activity-errors-nicely
  (is-submap {:status 400
              :body {:kixi.datastore.web-server/error :query-invalid}}
             (search-metadata (uuid) [::ms/Xfile-readX] nil nil nil {:coerce :exceptional})))

(deftest meta-read-activity-added-to-queries-by-default
  (let [uid (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata uid))]
    (when-success metadata-response
      (is-submap {:body {:items [{::ms/sharing {::ms/meta-read #{uid}}}]}}
                 (search-metadata uid [])))))

(defn first-item
  [resp]
  (first (get-in resp [:body :items])))

(defn shares
  [item activity]
  (set (get-in item [::ms/sharing activity])))

(deftest multiple-groups-can-have-read
  (let [uid (uuid)
        group1 (uuid)
        metadata-response (-> (create-metadata uid)
                              (update-in
                               [::ms/sharing ::ms/meta-read]
                               conj group1)
                              send-file-and-metadata)]
    (when-success metadata-response
      (let [resp (search-metadata uid [])]
        (when-success resp
          (is (= #{uid group1}
                 (shares (first-item resp) ::ms/meta-read)))))
      (let [resp (search-metadata group1 [])]
        (when-success resp
          (is (= #{uid group1}
                 (shares (first-item resp) ::ms/meta-read))))))))

(deftest novel-user-finds-nothing-when-there-is-something
  (let [uid (uuid)
        novel-group (uuid)
        metadata-response (send-file-and-metadata
                           (create-metadata uid))]
    (when-success metadata-response
      (is-submap {:status 200
                  :body {:items []}}
                 (search-metadata novel-group [])))))

(deftest search-uses-and-for-multiple-activities
  (let [uid (uuid)
        only-read-group (uuid)
        metadata-response (-> (create-metadata uid)
                              (update-in
                               [::ms/sharing ::ms/meta-read]
                               conj only-read-group)
                              send-file-and-metadata)]
    (when-success metadata-response
      (let [resp (search-metadata uid [::ms/meta-read ::ms/file-read])]
        (when-success resp
          (is (= #{uid only-read-group}
                 (shares (first-item resp) ::ms/meta-read)))
          (is (= #{uid}
                 (shares (first-item resp) ::ms/file-read)))))
      (let [resp (search-metadata only-read-group [::ms/meta-read])]
        (when-success resp
          (is (= #{uid only-read-group}
                 (shares (first-item resp) ::ms/meta-read)))
          (is (= #{uid}
                 (shares (first-item resp) ::ms/file-read)))))
      (is-submap {:status 200
                  :body {:items []}}
                 (search-metadata only-read-group [::ms/meta-read ::ms/file-read])
                 "activities should be AND'd together"))))

(defn add-meta-read
  [file-id uid new-group]
  (let [event (base/update-metadata-sharing
               uid uid
               file-id
               ::ms/sharing-conj 
               ::ms/meta-read
               new-group)]
    (if (= (:kixi.comms.event/key event)
           :kixi.datastore.file-metadata/updated)
      {:status 200}
      event)))

(deftest permissions-added-are-returned-by-search
  (let [uid (uuid)
        only-read-group (uuid)
        metadata-response (send-file-and-metadata (create-metadata uid))]
    (when-success metadata-response
      (let [resp (search-metadata uid [::ms/meta-read ::ms/meta-update])]
        (when-success resp
          (is (first-item resp))
          (is (= #{uid}
                 (shares (first-item resp) ::ms/meta-read)))
          (is (= #{uid}
                 (shares (first-item resp) ::ms/meta-update)))))
      
      (when-success (add-meta-read (::ms/id (:body metadata-response)) uid only-read-group)
        
        (let [resp (search-metadata uid [::ms/meta-read])]
          (when-success resp
            (is (first-item resp))
            (is (= #{uid only-read-group}
                   (shares (first-item resp) ::ms/meta-read)))))

        (let [resp (search-metadata only-read-group [::ms/meta-read])]
          (when-success resp
            (is (first-item resp))
            (is (= #{uid only-read-group}
                   (shares (first-item resp) ::ms/meta-read)))))))))

(defn remove-meta-read
  [file-id uid new-group]
  (let [event (base/update-metadata-sharing
               uid uid
               file-id
               ::ms/sharing-disj
               ::ms/meta-read
               new-group)]
    (if (= (:kixi.comms.event/key event)
           :kixi.datastore.file-metadata/updated)
      {:status 200}
      event)))

(deftest permissions-removed-no-longer-work
  (let [uid (uuid)
        only-read-group (uuid)
        metadata-response (-> (create-metadata uid)
                              (update-in
                               [::ms/sharing ::ms/meta-read]
                               conj only-read-group)
                              send-file-and-metadata)]
    (when-success metadata-response
      (let [resp (search-metadata uid [::ms/meta-read])]
        (when-success resp
          (is (= #{uid only-read-group}
                 (shares (first-item resp) ::ms/meta-read)))))
      (let [resp (search-metadata only-read-group [::ms/meta-read])]
        (when-success resp
          (is (= #{uid only-read-group}
                 (shares (first-item resp) ::ms/meta-read)))))
      
      (when-success (remove-meta-read (::ms/id (:body metadata-response)) uid only-read-group)    
        
        (let [resp (search-metadata uid [::ms/meta-read])]
          (when-success resp
            (is (= #{uid}
                   (shares (first-item resp) ::ms/meta-read)))))

        (let [resp (search-metadata only-read-group [::ms/meta-read])]
          (when-success resp
            (is (nil? (first-item resp)))))))))

(deftest search-returns-metadata-when-the-user-has-meta-read-and-uses-it-when-searching
  (doseq [activities (map set (subsets ms/activities))]
    (let [uid (uuid)
          activity->group (zipmap ms/activities
                                  (repeatedly uuid))
          metadata (update-in 
                    (reduce
                     (fn [md act]
                       (assoc-in md [::ms/sharing act] [(act activity->group)]))
                     (dissoc (create-metadata uid)
                             ::ms/sharing)
                     activities)
                    [::ms/sharing ::ms/meta-read]
                    #((comp vec cons) uid %))
          metadata-response (send-file-and-metadata
                             uid
                             uid
                             metadata)]
      (when-success metadata-response
        (doseq [search-activities (remove empty? (map set (subsets ms/activities)))]
          (let [search-result (search-metadata (mapv activity->group search-activities) search-activities)]
            (if (and (activities ::ms/meta-read)
                     (search-activities ::ms/meta-read))
              (is-submap {:status 200
                          :body {:paging {:total 1 :count 1 :index 0}}}
                         search-result
                         (str "Failed to find metadata with " search-activities " when meta has " activities))
              (is-submap {:status 200
                          :body {:paging {:total 0 :count 0 :index 0}}}
                         search-result
                         (str "Found metadata with " search-activities " when meta has " activities)))))))))
