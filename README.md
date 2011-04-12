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

Serializing non-String types

    user> (put-row ks "Users" "Paul" {"age" 30})
    #<MutationResultImpl MutationResult took (2us) for query (n/a) on host: localhost(127.0.0.1):9160>
    user> (get-rows ks "Users" ["Paul"] {:v-serializer :integer})
    ({:key "Paul", :columns {"age" 30}})

## TODO

* Better support different Hector query types- multimethod dispatch
  based on arity of pk and c args?
* Turn off spurious logging by default
* Serialization of non-String types
* Better support of CassandraHostConfigurator
* Type hints to avoid reflecting
* Plenty more :)

## License

Copyright (c) Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
