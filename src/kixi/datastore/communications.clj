(ns kixi.datastore.communications)

(defprotocol Communications
  (submit [this msg])
  (attach-pipeline-processor [this selector processor])
  (attach-sink-processor [this selector processor])
  (detach-processor [this processor]))

