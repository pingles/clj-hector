# clj-hector

A simple Clojure client for Cassandra that wraps Hector. The 0.2.1 release was built against Clojure 1.4.0.

Current build status: ![Build status](https://secure.travis-ci.org/pingles/clj-hector.png)

## Installation

Add the following to your `project.clj`

    :dependencies [[org.clojars.paul/clj-hector "0.3.1"]]

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
    (drop-keyspace cluster "Keyspace Name")

### Basic retrieval of rows

    (def c (cluster "Pauls Cluster" "localhost"))
    (def ks (keyspace c "Twitter"))
    (get-rows ks "Users" ["paul"] :n-serializer :string)

    user> (-> (cluster "Pauls Cluster" "localhost")
              (keyspace "Twitter")
              (get-rows "Users" ["paul"] :n-serializer :string))
    ({"paul" {"age" #<byte[] [B@324a897c>, "login" #<byte[] [B@3b8845af>}})

It's also possible to query for column slices

    user> (-> (cluster "Pauls Cluster" "localhost")
              (keyspace "Twitter")
              (get-columns "Users" "paul" ["age" "login"] :n-serializer :string))

### Serializing non-String types

    user> (put ks "Users" "Paul" {"age" 30})
    #<MutationResultImpl MutationResult took (2us) for query (n/a) on host: localhost(127.0.0.1):9160>
    user> (get-rows ks "Users" ["Paul"] :n-serializer :string :v-serializer :integer)
    ({"Paul" {"age" 30}})

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

    user> (put ks "UserRelationships" "paul" {"SuperCol" {"k" "v"} "SuperCol2" {"k2" "v2"}} :type :super)
    #<MutationResultImpl MutationResult took (6us) for query (n/a) on host: localhost(127.0.0.1):9160>

Retrieving super columns with `get-super-rows`:

    user> (get-super-rows ks "UserRelationships" ["paul"] ["SuperCol" "SuperCol2"] :s-serializer :string :n-serializer :string :v-serializer :string)
    ({"paul" ({"SuperCol", {"a" "1", "k" "v"}} {"SuperCol2", {"k2" "v2"}})})

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
    ({"paul" ()})

TODO: In the above query, a row is returned despite having no results. This
should probably just return an empty sequence.

### Query metadata

Hector exposes data about how long queries took to execute (and on which host). This is provided as metadata on the query result maps:

    user> (meta (get-rows ks "Users" ["Paul"] {:n-serializer :string :v-serializer :integer}))
    {:exec_us 2, :host #<CassandraHost localhost(127.0.0.1):9160>}

## Experimental Schema Querying

`clj-hector` allows you to provide default schema settings for the specified column families (see `./test/clj_hector/test/schema.clj` for examples).

For example, when operating with the MyColumnFamily column family, you can provide default name and value serializers as follows:

    (def MyColumnFamily [:name "MyColumnFamily"
                         :n-serializer :string
                         :v-serializer :string])

Then, when querying, wrap the functions with the `with-schema` macro:

    (with-schemas [MyColumnFamily]
      (put ks "MyColumnFamily" "row-key" {"k" "v"})
      (get-rows ks "MyColumnFamily" ["row-key"])))

Note that it's still very early days- all suggestions and forks are welcome!

## TODO

* Better support different Hector query types- multimethod dispatch
  based on arity of pk and c args?
* Paging support for queries (somehow wiring into chunked sequences?)
* Better support of CassandraHostConfigurator
* Refactoring

## License

Copyright (c) Paul Ingles

Distributed under the Eclipse Public License, the same as Clojure.
