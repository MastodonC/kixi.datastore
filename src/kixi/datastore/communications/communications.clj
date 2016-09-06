(ns kixi.datastore.communications.communications)


(defprotocol Communications
  (new-metadata [this meta-data])
  (update-metadata [this meta-update])
  (attach-processor [this selector processor])
  (detach-processor [this processor]))

(defn metadata-new-selector
  [msg]
  (= :metadata-new
     (:type msg)))

(defn metadata-update-selector
  [msg]
  (= :metadata-update
     (:type msg)))
