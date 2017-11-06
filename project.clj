(def metrics-version "2.7.0")
(def slf4j-version "1.7.21")
(defproject kixi.datastore "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[aero "1.1.2"]
                 [aleph "0.4.2-alpha8"]
                 [amazonica "0.3.92" :exclusions [ch.qos.logback/logback-classic
                                                  com.amazonaws/aws-java-sdk
                                                  com.fasterxml.jackson.dataformat/jackson-dataformat-cbor
                                                  commons-logging
                                                  com.fasterxml.jackson.core/jackson-databind
                                                  com.fasterxml.jackson.core/jackson-core
                                                  org.apache.httpcomponents/httpclient
                                                  joda-time]]
                 [com.amazonaws/aws-java-sdk "1.11.53" :exclusions [joda-time]]
                 [bidi "2.0.12"]
                 [byte-streams "0.2.3"]
                 [clj-http "3.7.0"]
                 [clj-time "0.14.0"]
                 [clojure-csv/clojure-csv "2.0.2"]
                 [cheshire "5.8.0"]
                 [cider/cider-nrepl "0.15.1"]
                 [com.cognitect/transit-clj "0.8.300"]
                 [com.fzakaria/slf4j-timbre "0.3.7"]
                 [com.rpl/specter "1.0.3"]
                 [com.stuartsierra/component "0.3.2"]
                 [com.taoensso/faraday "1.9.0" :exclusions [com.amazonaws/aws-java-sdk-dynamodb
                                                            com.taoensso/encore]]
                 [com.taoensso/timbre "4.10.0"]
                 [com.taoensso/encore "2.92.0"]
                 [digest "1.4.6"]
                 [environ "1.1.0"]
                 [kixi/kixi.comms "0.2.27"]
                 [kixi/kixi.log "0.1.5"]
                 [kixi/kixi.metrics "0.4.1"]
                 [kixi/joplin.core "0.3.10-SNAPSHOT"]
                 [kixi/joplin.dynamodb "0.3.10-SNAPSHOT"]
                 [manifold "0.1.6-alpha1"]
                 [medley "1.0.0"]
                 [metrics-clojure ~metrics-version]
                 [metrics-clojure-jvm ~metrics-version]
                 [metrics-clojure-ring ~metrics-version]
                 [metosin/ring-swagger "0.22.10"]
                 [org.clojure/clojure "1.9.0-beta1"]
                 [org.clojure/core.async "0.3.443"]
                 [org.slf4j/log4j-over-slf4j ~slf4j-version]
                 [org.slf4j/jul-to-slf4j ~slf4j-version]
                 [org.slf4j/jcl-over-slf4j ~slf4j-version]
                 [org.clojure/test.check "0.9.0"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/tools.analyzer "0.6.9"]
                 [spootnik/signal "0.2.1"]
                 [yada "1.1.44" :exclusions [com.fasterxml.jackson.core/jackson-core]]]

  :jvm-opts ["-Xmx2g"]

  :repl-options {:init-ns user}

  :exclusions [cheshire]

  :test-selectors {:integration :integration
                   :acceptance :acceptance}

  :global-vars {*warn-on-reflection* true
                *assert* false}

  :plugins [[lein-test-out "0.3.1"]]

  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/math.combinatorics "0.1.4"]
                                  [com.gfredericks/test.chuck "0.2.8"]
                                  [criterium "0.4.4"]]}
             :uberjar {:aot [kixi.datastore.bootstrap]
                       :uberjar-name "kixi.datastore-standalone.jar"}})
