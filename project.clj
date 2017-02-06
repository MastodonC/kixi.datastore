(def metrics-version "2.7.0")
(def slf4j-version "1.7.21")
(defproject kixi.datastore "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[aero "1.0.0"]
                 [aleph "0.4.2-alpha8"]
                 [amazonica "0.3.74" :exclusions [ch.qos.logback/logback-classic
                                                  com.amazonaws/aws-java-sdk
                                                  com.fasterxml.jackson.dataformat/jackson-dataformat-cbor
                                                  commons-logging
                                                  com.fasterxml.jackson.core/jackson-databind
                                                  com.fasterxml.jackson.core/jackson-core
                                                  org.apache.httpcomponents/httpclient
                                                  joda-time]]
                 [com.amazonaws/aws-java-sdk "1.11.53" :exclusions [joda-time]]
                 [bidi "2.0.12"]
                 [byte-streams "0.2.2"]
                 [clj-http "2.2.0"]
                 [clj-time "0.12.0"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [cheshire "5.6.3"]
                 [com.cognitect/transit-clj "0.8.288"]
                 [com.izettle/dropwizard-metrics-influxdb "1.1.6" :exclusions [ch.qos.logback/logback-classic 
                                                                               com.fasterxml.jackson.core/jackson-core
                                                                               joda-time]]
                 [com.fzakaria/slf4j-timbre "0.3.2"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/timbre "4.7.0"]
                 [digest "1.4.4"]
                 [clojurewerkz/elastisch "3.0.0-beta1"]
                 [environ "1.1.0"]
                 [kixi/kixi.comms "0.1.25"]
                 [joplin.core "0.3.9"]
                 [joplin.elasticsearch "0.3.9"]
                 [manifold "0.1.6-alpha1"]
                 [medley "0.8.3"]
                 [metrics-clojure ~metrics-version]
                 [metrics-clojure-jvm ~metrics-version]
                 [metrics-clojure-ring ~metrics-version]
                 [metosin/ring-swagger "0.22.10"]
                 [org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/core.async "0.2.391"]
                 [org.clojure/tools.analyzer "0.6.9"]
                 [org.slf4j/log4j-over-slf4j ~slf4j-version]
                 [org.slf4j/jul-to-slf4j ~slf4j-version]
                 [org.slf4j/jcl-over-slf4j ~slf4j-version]
                 [org.clojure/test.check "0.9.0"]
                 [yada "1.1.44" :exclusions [com.fasterxml.jackson.core/jackson-core]]]

  :jvm-opts ["-Xmx2g"]

  :repl-options {:init-ns user}

  :exclusions [cheshire]

  :test-selectors {:default (complement :integration)
                   :integration :integration}

  :global-vars {*warn-on-reflection* true
                *assert* false}

  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/math.combinatorics "0.1.3"]
                                  [com.gfredericks/test.chuck "0.2.7"]
                                  [criterium "0.4.4"]]}
             :uberjar {:aot [kixi.datastore.bootstrap]
                       :uberjar-name "kixi.datastore-standalone.jar"}})
