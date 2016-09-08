(ns kixi.datastore.schemastore.inmemory
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.communications :refer [Communications]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

