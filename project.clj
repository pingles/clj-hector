(defproject org.clojars.paul/clj-hector "0.3.2" 
  :dependencies [[org.hectorclient/hector-core "1.1-2"]
                 [org.slf4j/slf4j-api "1.7.5"]
                 [org.clojure/clojure "1.4.0"]
                 [joda-time/joda-time "2.0"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "1.7.5"]
                                  [org.apache.cassandra/cassandra-all "1.1.6"]]}}
  :exclusions [org.slf4j/slf4j-log4j12]
  :resource-paths ["test/resources"]
  :min-lein-version "2.0.0"
  :description "Wrapper for Hector Cassandra client")
