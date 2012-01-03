(defproject org.clojars.paul/clj-hector "0.0.4"
  :description "Wrapper for Hector Cassandra client"
  :resources-path "test/resources"
  :dev-dependencies [[org.slf4j/slf4j-simple "1.6.3"]
                     [org.apache.cassandra/cassandra-all "1.0.5"]]
  :dependencies [[me.prettyprint/hector-core "1.0-2" :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.slf4j/slf4j-api "1.6.1"]
                 [org.clojure/clojure "1.2.1"]
                 [joda-time/joda-time "2.0"]])
