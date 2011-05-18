(defproject org.clojars.paul/clj-hector "1.0.0-SNAPSHOT"
  :description "Wrapper for Hector Cassandra client"
  :dev-dependencies [[swank-clojure "1.2.1"]
                     [org.slf4j/slf4j-nop "1.5.8"]
                     [lein-clojars "0.6.0"]]
  :dependencies [[me.prettyprint/hector-core "0.7.0-29"]
                 [org.slf4j/slf4j-api "1.5.8"] 
                 [org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]])
