(ns kixi.datastore.communications.communications)

(defprotocol Communications
  (submit-metadata [this metadata])
  (attach-pipeline-processor [this selector processor])
  (attach-sink-processor [this selector processor])
  (detach-processor [this processor]))

