(defproject org.clojars.paul/clj-hector "0.0.4"
  :description "Wrapper for Hector Cassandra client"
  :dev-dependencies [[org.slf4j/slf4j-nop "1.6.1"]]
  :dependencies [[me.prettyprint/hector-core "1.0-1" :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.slf4j/slf4j-api "1.6.1"]
                 [org.clojure/clojure "1.2.1"]
                 [joda-time/joda-time "2.0"]])
