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

(def sharing-level->actions
  {:file-sharing {:read {dload-file true}}
   :file-metadata-sharing {:visible {get-metadata false
;                                     update-metadata false
                                     }
                           :read {get-metadata true
;                                  update-metadata false
                                  }
                           :update {get-metadata true
;                                    update-metadata true
                                    }}})

(deftest user-with-file-read-metadata-update-can-read
  true)
