(ns kixi.datastore.schemaextracter
  (:require [byte-streams :as bs]
            [clojure-csv.core :as csv :refer [parse-csv]]
            [clojure.string :refer [lower-case]]
            [com.stuartsierra.component :as component]
            [kixi.datastore.communications
             :refer [attach-pipeline-processor
                     detach-processor]]
            [kixi.datastore.documentstore
             :refer [retrieve]]
            [medley.core :refer [find-first assoc-some]]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [clojure.java.io :as io])
  (:import [java.io File]))

(defn metadata->file
  [documentstore metadata]
  (let [id (get-in metadata [:id])
        f (File/createTempFile id ".tmp")]
    (.deleteOnExit f)
    (bs/transfer
     (retrieve documentstore id)
     f)
    f))

(defn requires-schema-extraction?
  [metadata]
  (and
   (get-in metadata [:structural-validation :valid])
   ((complement :schema) metadata)))


(def gss-code {:header {:matcher #(some->> % lower-case (re-matches #"^gss[\.\-\ ]?code$"))
                        :contractions []}
               :value {:matcher (partial re-matches #"^[A-Z]\d{11}$")
                       :contractions []}
               :schema "gss-code"})

(def oa-code {:header {:matcher (fn [h]
                                  (when-let [t (some->> h lower-case)]
                                    (first
                                     (keep
                                      #(re-matches % t)
                                      [#"^output area$" 
                                       #"^oa$"
                                       #"^output area code$" 
                                       #"^oa code$"]))))
                       :contractions []}
              :value {:matcher (partial re-matches #"^\d\d[A-Z]{4}\d{4}$")
                      :contractions []}
              :schema "output-area-code"})

(def ons-oa-sub-group  {:header {:matcher (fn [h]
                                            (when-let [t (some->> h lower-case)]
                                              (first
                                               (keep
                                                #(re-matches % t)
                                                [#"^oac-subgroup$" 
                                                 #"^on-oa-subgroup$"
                                                 #"^oa-subgroup-classification$"]))))
                                 :contractions []}
                        :value {:matcher (partial re-matches #"^\d[a-z]\d$")
                                :contractions []}
                        :schema "ons-output-area-sub-group"})

(def oac-super-group  {:header {:matcher (fn [h]
                                           (when-let [t (some->> h lower-case)]
                                             (first
                                              (keep
                                               #(re-matches % t)
                                               [#"^oac-supergroup$"]))))
                                :contractions []}
                       :value {:matcher (partial re-matches #"^\d$")
                               :contractions []}
                       :schema "output-area-super-group"})

(def ward-code {:header {:matcher #(some->> % lower-case (re-matches #"^ward[\.\-\ ]?code$"))
                         :contractions []}
                :value {:matcher (partial re-matches #"^\d\d[A-Z]{4}$")
                        :contractions []}
                :schema "ward-code"})

(def numeric {:header {:matcher (constantly true)
                       :contractions []}
              :value {:matcher (partial re-matches #"^\d+\.?\d*$")
                      :contractions [oac-super-group]}
              :schema "numeric"})

(def string {:header {:matcher (constantly true)
                      :contractions [gss-code oa-code ons-oa-sub-group oac-super-group]}
             :value {:matcher (constantly true)
                     :contractions [numeric gss-code oa-code ward-code]}
             :schema "string"})

(defn schema-for
  ([method subject]
   (first
    (schema-for method [string] subject)))
  ([method selectors subject]
   (when-let [best-guesses (seq
                            (filter 
                             (fn [selector] 
                               (some->> subject ((get-in selector [method :matcher])))) 
                             selectors))]
     (lazy-cat (schema-for method (mapcat #(get-in % [method :contractions]) best-guesses) 
                           subject)
               best-guesses))))

(defn line
  [position file]
  (with-open [f (io/reader file)]
    (position (line-seq f))))

(def first-line (partial line first))
(def second-line (partial line second))

(defn header-details
  [header-line]
  (let [header-names (first (parse-csv header-line))]
    {:header-names header-names
     :header-schemas (zipmap header-names
                             (map (partial schema-for :header [string]) header-names))}))

(defn value-schema
  [value-line]
  (some->> value-line
           parse-csv
           first
           (map (partial schema-for :value [string]))))

(defn extract-schema-csv
  [documentstore metadata]
  (let [^File file (metadata->file documentstore metadata)]
    (try
      (-> {}
          (assoc-some :header (when (:header-line metadata)
                                (-> file
                                    first-line
                                    header-details)))
          (assoc-some :value (->> file
                                  second-line
                                  value-schema)))
      (finally
        (.delete file)))))


(defn extract-schema
  [documentstore]
  (fn [metadata]
    (assoc metadata
           :schema
           (case (:type metadata)
             :csv (extract-schema-csv documentstore metadata)))))

(defprotocol ISchemaExtracter)

(defrecord SchemaExtracter
    [communications documentstore extract-schema-fn]
    ISchemaExtracter
    component/Lifecycle
    (start [component]
      (if-not extract-schema-fn
        (let [es-fn (extract-schema documentstore)]
          (info "Starting SchemaExtracter")
          (attach-pipeline-processor communications
                                     requires-schema-extraction?
                                     es-fn)
          (assoc component
                 :extract-schema-fn es-fn))
        component))
    (stop [component]
      (info "Stopping SchemaExtracter")
      (if extract-schema-fn
        (do
          (detach-processor communications
                           extract-schema-fn)
          (dissoc component 
                  :extract-schema-fn))
        component)))
