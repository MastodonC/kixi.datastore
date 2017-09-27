(ns kixi.datastore.schema-creator
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [kixi.datastore.transport-specs :as ts]
            [kixi.comms :as c]
            [kixi.datastore.schemastore :as ss]))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn schema-validator
  [{:keys [kixi.comms.command/payload] :as cmd}]
  (let [new-id (uuid)
        raw-schema-req (assoc payload
                              ::ss/id new-id)
        internal-sr (ts/schema-transport->internal raw-schema-req)]
    (if (s/valid?
         ::ss/create-schema-request
         internal-sr)
      {:kixi.comms.event/key :kixi.datastore.schema/created
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/partition-key new-id
       :kixi.comms.event/payload internal-sr}
      {:kixi.comms.event/key :kixi.datastore.schema/rejected
       :kixi.comms.event/version "1.0.0"
       :kixi.comms.event/partition-key new-id
       :kixi.comms.event/payload {:reason :invalid-schema
                                  :schema raw-schema-req
                                  :explanation (s/explain-data ::ss/create-schema-request
                                                                internal-sr)}})))

(defn attach-command-handler
  [comms]
  (c/attach-command-handler!
   comms
   :kixi.datastore/schemastore-creator
   :kixi.datastore.schema/create
   "1.0.0" schema-validator))
