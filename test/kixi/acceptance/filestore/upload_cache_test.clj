(ns kixi.acceptance.filestore.upload-cache-test
  {:acceptance true}
  (:require [clojure.test :refer :all]
            [kixi.datastore.time :as t]
            [kixi.datastore.filestore :as fs]
            [kixi.datastore.filestore.upload :as up]
            [kixi.datastore.application :as app]
            [kixi.integration.base :as base :refer :all]))

(use-fixtures :once
  (create-cycle-system-fixture [:filestore-upload-cache]))

(deftest basic-functionality-test
  (let [fuc (:filestore-upload-cache @app/system)
        file-id (uuid)
        upload-id (uuid)
        mup? true
        user {:kixi.user/username "foo@bar.com"
              :kixi.user/id (uuid)}
        created-at (t/timestamp)]
    (fs/put-item! fuc file-id mup? user upload-id created-at)
    (let [item (fs/get-item fuc file-id)]
      (is item)
      (when item
        (is (= file-id (::fs/id item)))
        (is (= upload-id (::up/id item)))
        (is (= mup? (::up/mup? item)))
        (is (= user (:kixi/user item)))
        (is (= created-at (::up/created-at item)))

        (fs/delete-item! fuc file-id)
        (is (not (fs/get-item fuc file-id)))))))
