(defproject org.clojars.paul/clj-hector "0.2.3-SNAPSHOT" 
  :dependencies [[me.prettyprint/hector-core "1.0-3" :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.slf4j/slf4j-api "1.6.1"]
                 [org.clojure/clojure "1.4.0"]
                 [joda-time/joda-time "2.0"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "1.6.3"]
                                  [org.apache.cassandra/cassandra-all "1.0.5"]]}}
  :resource-paths ["test/resources"]
  :min-lein-version "2.0.0"
  :description "Wrapper for Hector Cassandra client")
