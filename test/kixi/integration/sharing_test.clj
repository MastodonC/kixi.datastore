(ns kixi.integration.sharing-test
  (:require [clojure.test :refer :all]            
            [kixi.integration.base :refer :all]))


(def metadata-file-schema-id (atom nil))
(def metadata-file-schema {:name ::metadata-file-schema
                           :type "list"
                           :definition [:cola {:type "integer"}
                                        :colb {:type "integer"}]})
(defn setup-schema
  [all-tests]
  (let [r (post-spec metadata-file-schema)]
    (if (= 202 (:status r))
      (reset! metadata-file-schema-id (extract-id r))
      (throw (Exception. (str "Couldn't post metadata-file-schema. Resp: " r))))
    (wait-for-url (get-in r [:headers "Location"])))
  (all-tests))

(use-fixtures :once cycle-system-fixture setup-schema)

(defn authorized
  [resp]
  (#{200 201}
   (:status resp)))

(defn unauthorized 
  [resp]
  (= 401
     (:status resp)))

(def sharing-level->actions
  {:file-sharing {:read {get-file authorized
                         get-metadata unauthorized}}
})
{   :file-metadata-sharing {:visible {get-metadata false
                                     dload-file-by-id false
;                                     update-metadata false
                                     }
                           :read {get-metadata true
                                  dload-file-by-id false
;                                  update-metadata false
                                  }
                           :update {get-metadata true
                                    dload-file-by-id false
;                                    update-metadata true
                                    }}}

(deftest explore-sharing-level->actions
  (let [post (partial post-file-flex
                      :file-name "./test-resources/metadata-one-valid.csv"
                      :schema-id @metadata-file-schema-id)]
    (doseq [[share levels] sharing-level->actions]
      (doseq [[level actions] levels]
        (let [user-id (uuid)
              pfr (post :user-id user-id
                        share {level [user-id]})]
          (is-submap {:status 201}
                     pfr)
          (when (= 201 (:status pfr))
            (let [file-id (extract-id pfr)]
              (doseq [[action result-fn] actions]
                (is (result-fn
                     (action file-id))
                    (str "Is " action " " result-fn " when " share " at " level " provided"))))))))))
