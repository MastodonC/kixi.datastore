(ns kixi.datastore.file
  (:import [java.io
            File
            OutputStream]))

(defn  temp-file
  ^File [name]
  (doto (File/createTempFile name ".tmp")
    .deleteOnExit))

(defn close
  [^OutputStream os]
  (.close os))
