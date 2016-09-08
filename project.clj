(def metrics-version "2.7.0")
(def slf4j-version "1.7.21")
(defproject kixi.datastore "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[aero "1.0.0"]
                 [aleph "0.4.1"]
                 [amazonica "0.3.74" :exclusions [com.fasterxml.jackson.dataformat/jackson-dataformat-cbor
                                                  commons-logging
                                                  com.fasterxml.jackson.core/jackson-core]]
                 [bidi "2.0.9"]
                 [byte-streams "0.2.2"]
                 [clj-http "2.2.0"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [com.izettle/dropwizard-metrics-influxdb "1.1.6" :exclusions [ch.qos.logback/logback-classic]]
                 [com.fzakaria/slf4j-timbre "0.3.2"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/timbre "4.7.0"]
                 [digest "1.4.4"]
                 [medley "0.8.3"]
                 [metrics-clojure ~metrics-version]
                 [metrics-clojure-jvm ~metrics-version]
                 [metrics-clojure-ring ~metrics-version]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.385"]
                 [org.slf4j/log4j-over-slf4j ~slf4j-version]
                 [org.slf4j/jul-to-slf4j ~slf4j-version]
                 [org.slf4j/jcl-over-slf4j ~slf4j-version]
                 [yada "1.1.33"]]

  :test-selectors {:default (fn [m] (not (:integration m)))
                   :integration :integration}

  :global-vars {*warn-on-reflection* true
                *assert* false}

  :profiles {:uberjar {:aot [kixi.datastore.bootstrap]
                       :uberjar-name "kixi.datastore-standalone.jar"}})
