# clj-hector

A simple Clojure client for Cassandra that wraps Hector

## Usage

Schema Manipulation

    (def cluster (cluster "Pauls Cluster" "localhost"))
    (add-keyspace cluster
                  {:name "Keyspace Name"
                   :replication 3
                   :column-families [{:name "a"}
                                     {:name "b"
                                      :comparator :long}]})
    (add-column-family cluster "Keyspace Name" {:name "c"})
    (drop-keyspace cluster "Keyspace Name)

Basic retrieval of rows

    (def c (cluster "Pauls Cluster" "localhost"))
    (def ks (keyspace c "Twitter"))
    (get-rows ks "Users" ["paul"])

    (-> (cluster "Pauls Cluster" "localhost")
        (keyspace "Twitter")
        (get-rows "Users" ["paul"]))

## TODO

* Turn off spurious logging by default
* Serialization of non-String types
* Better support of CassandraHostConfigurator
* Type hints to avoid reflecting
* Plenty more :)

## License

Copyright (c) Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
