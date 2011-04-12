# clj-hector

A simple Clojure client for Cassandra that wraps Hector

## Usage

### Schema Manipulation

    (def cluster (cluster "Pauls Cluster" "localhost"))
    (add-keyspace cluster
                  {:name "Keyspace Name"
                   :replication 3
                   :column-families [{:name "a"}
                                     {:name "b"
                                      :comparator :long}]})
    (add-column-family cluster "Keyspace Name" {:name "c"})
    (drop-keyspace cluster "Keyspace Name)

### Basic retrieval of rows

    (def c (cluster "Pauls Cluster" "localhost"))
    (def ks (keyspace c "Twitter"))
    (get-rows ks "Users" ["paul"] {:n-serializer :string})

    user> (-> (cluster "Pauls Cluster" "localhost")
              (keyspace "Twitter")
              (get-rows "Users" ["paul"] {:n-serializer :string}))
    ({:key "paul", :columns {"age" #<byte[] [B@324a897c>, "login" #<byte[] [B@3b8845af>}})

### Serializing non-String types

    user> (put-row ks "Users" "Paul" {"age" 30})
    #<MutationResultImpl MutationResult took (2us) for query (n/a) on host: localhost(127.0.0.1):9160>
    user> (get-rows ks "Users" ["Paul"] {:n-serializer :string :v-serializer :integer})
    ({:key "Paul", :columns {"age" 30}})
    
### Query metadata

Hector exposes data about how long queries took to execute (and on which host). This is provided as metadata on the query result maps:

    user> (meta (get-rows ks "Users" ["Paul"] {:n-serializer :string :v-serializer :integer}))
    {:exec_us 2, :host #<CassandraHost localhost(127.0.0.1):9160>}

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
