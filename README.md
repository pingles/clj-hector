# clj-hector

A simple Clojure client for Cassandra that wraps Hector

## Installation

Add the following to your `project.clj`

    :dev-dependencies [[org.clojars.paul/clj-hector "1.0.0-SNAPSHOT"]]

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
    (get-rows ks "Users" ["paul"] :n-serializer :string)

    user> (-> (cluster "Pauls Cluster" "localhost")
              (keyspace "Twitter")
              (get-rows "Users" ["paul"] :n-serializer :string))
    ({:key "paul", :columns {"age" #<byte[] [B@324a897c>, "login" #<byte[] [B@3b8845af>}})

It's also possible to query for column slices

    user> (-> (cluster "Pauls Cluster" "localhost")
              (keyspace "Twitter")
              (get-columns "Users" "paul" ["age" "login"] :n-serializer :string))

### Serializing non-String types

    user> (put-row ks "Users" "Paul" {"age" 30})
    #<MutationResultImpl MutationResult took (2us) for query (n/a) on host: localhost(127.0.0.1):9160>
    user> (get-rows ks "Users" ["Paul"] :n-serializer :string :v-serializer :integer)
    ({:key "Paul", :columns {"age" 30}})

The following serializers are supported

* `:string`
* `:integer`
* `:long`
* `:bytes`

### Super Columns

Firstly, the column family will need to support super columns.

    user> (add-column-family cluster "Keyspace Name" {:name "UserRelationships"
                                                      :type :super})

Storing super columns works using a nested map structure:

    user> (put-row ks "UserRelationships" "paul" {"SuperCol" {"k" "v"} "SuperCol2" {"k2" "v2"}})
    #<MutationResultImpl MutationResult took (6us) for query (n/a) on host: localhost(127.0.0.1):9160>

Retrieving super columns with `get-super-rows`:

    user> (get-super-rows ks "UserRelationships" ["paul"] ["SuperCol" "SuperCol2"] :s-serializer :string :n-serializer :string :v-serializer :string)
    ({:key "paul", :super-columns ({:name "SuperCol", :columns {"a"
    "1", "k" "v"}} {:name "SuperCol2", :columns {"k2" "v2"}})})

In the above example, note the addition of the s-serializer option:
this controls how super column names should be deserialized.

You can also query for a sequence of columns:

    user> (get-super-columns ks "UserRelationships" "paul" "SuperCol" ["a" "k"] :s-serializer :string :n-serializer :string :v-serializer :string)
    {"a" "1", "k" "v"}

### Deleting Rows

It's possible to delete all columns identified by keys with the
`delete-rows` function. This works with both super-column families and
regular column families.

To delete the example above:

    user> (delete-rows ks "UserRelationships" ["paul"])

    user> (get-super-columns ks "UserRelationships" "paul" "SuperCol" ["a" "k"] :s-serializer :string :n-serializer :string :v-serializer :string)
    {}

    user> (get-super-rows ks "UserRelationships" ["paul"] ["SuperCol" "SuperCol2"] :s-serializer :string :n-serializer :string :v-serializer :string)
    ({:key "paul", :super-columns ()})

TODO: In the above query, a row is returned despite having no results. This
should probably just return an empty sequence.

### Query metadata

Hector exposes data about how long queries took to execute (and on which host). This is provided as metadata on the query result maps:

    user> (meta (get-rows ks "Users" ["Paul"] {:n-serializer :string :v-serializer :integer}))
    {:exec_us 2, :host #<CassandraHost localhost(127.0.0.1):9160>}

## TODO

* Better support different Hector query types- multimethod dispatch
  based on arity of pk and c args?
* Super columns
  * Add suport for `delete-columns`
* Paging support for queries (somehow wiring into chunked sequences?)
* Better support of CassandraHostConfigurator
* Type hints to avoid reflecting
* Refactoring
* Plenty more :)

## License

Copyright (c) Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
