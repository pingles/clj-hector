(ns clj-hector.test.cassandra-helper
  (:import [org.apache.cassandra.service EmbeddedCassandraService]
           [java.util UUID])
  (:use [clojure.test]
        [clj-hector.core :only (cluster keyspace)]
        [clj-hector.ddl :only (add-keyspace drop-keyspace)]))

;; use an in-process cassandra daemon
;; make tests easier to specify and work
;; with isolated keyspaces

(defn create-daemon
  []
  (doto (EmbeddedCassandraService. )
    (.start)))

(defonce daemon (create-daemon))

(defn embedded-cluster
  []
  (cluster "Embedded Test Cluster" "127.0.0.1" 9960))

(defmacro with-test-keyspace
  "Creates a test keyspace which is dropped after the
   tests are executed."
  [name column-families & body]
  `(let [ks-name# (.replace (str "ks" (UUID/randomUUID)) "-" "")
         cluster# (embedded-cluster)]
     (add-keyspace cluster# {:name ks-name#
                             :strategy :simple
                             :replication 1
                             :column-families ~column-families})
     (let [~name (keyspace cluster# ks-name#)]
       (try ~@body
            (finally (drop-keyspace cluster# ks-name#))))))

(defmacro with-test-cluster
  "Creates a connection to the embedded daemon and initializes
   a cluster."
  [name & body]
  `(let [~name (embedded-cluster)]
     ~@body))
