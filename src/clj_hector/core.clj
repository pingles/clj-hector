(ns clj-hector.core
  ^{:author "Paul Ingles"
    :doc "Hector-based Cassandra client"}
  (:require [clj-hector.serialize :as s])
  (:use [clojure.contrib.java-utils :only (as-str)]
        [clojure.contrib.def :only (defnk)])
  (:import [java.io Closeable]
           [me.prettyprint.hector.api.mutation Mutator]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.hector.api.query Query]
           [me.prettyprint.cassandra.service CassandraHostConfigurator]
           [me.prettyprint.cassandra.serializers TypeInferringSerializer]))

(def ^:dynamic *schemas* {})

(def type-inferring (TypeInferringSerializer/get))

(defn cluster
  "Connects to Cassandra cluster"
  ([cluster-name host]
     (cluster cluster-name host 9160))
  ([cluster-name host port]
     (HFactory/getOrCreateCluster cluster-name
                                  (CassandraHostConfigurator. (str host ":" port)))))
(defn keyspace
  "Connects the client to the specified Keyspace. All other interactions
   with Cassandra are performed against this keyspace."
  [cluster name]
  (HFactory/createKeyspace name cluster))

(defnk create-column
  "Creates Column and SuperColumns.

   Serializers for the super column name, column name, and column value default to an instance of TypeInferringSerializer.

   Examples: (create-column \"name\" \"a value\")  (create-column \"super column name\" {\"name\" \"value\"})"
  [name value :n-serializer type-inferring :v-serializer type-inferring :s-serializer type-inferring]
  (if (map? value)
    (let [cols (map (fn [[n v]] (create-column n v :n-serializer n-serializer :v-serializer v-serializer)) value)]
      (HFactory/createSuperColumn name cols s-serializer n-serializer v-serializer))
    (HFactory/createColumn name value n-serializer v-serializer)))

(defn put
  "Stores values in columns in map m against row key pk"
  [ks cf pk m]
  (let [^Mutator mut (HFactory/createMutator ks type-inferring)]
    (doseq [[k v] m] (.addInsertion mut pk cf (create-column k v)))
    (.execute mut)))

(defnk create-counter-column
  [name value :n-serializer type-inferring]
  (HFactory/createCounterColumn name value n-serializer))

(defn put-counter
  "Stores a counter value. Column Family must have the name validator
   type set to :counter (CounterColumnType).

   pk is the row key. m is a map of column names and the (long) counter
   value to store.

   Counter columns allow atomic increment/decrement."
  [ks cf pk m]
  (let [^Mutator mut (HFactory/createMutator ks type-inferring)]
    (doseq [[n v] m]
      (.addCounter mut pk cf (create-counter-column n v)))
    (.execute mut)))

(defn- execute-query [^Query query]
  (s/to-clojure (.execute query)))

(defn- schema-options
  [column-family]
  (get *schemas* column-family))

(defn- extract-options
  [opts cf]
  (let [defaults {:s-serializer :bytes
                  :n-serializer :bytes
                  :v-serializer :bytes
                  :start nil
                  :end nil
                  :reversed false
                  :limit Integer/MAX_VALUE}]
    (merge defaults (apply hash-map opts) (schema-options cf))))

(defn get-super-rows
  "In keyspace ks, from Super Column Family cf, retrieve the rows identified by pks. Executed
   as a slice query. The range of columns to select can be provided through the optional named
   arguments :start and :end.

   Optional: scs can be a sequence of super column names to retrieve columns for."
  [ks cf pks scs & o]
  (let [opts (extract-options o cf)]
    (execute-query (doto (HFactory/createMultigetSuperSliceQuery ks
                                                                 (s/serializer (first pks))
                                                                 (s/serializer (:s-serializer opts))
                                                                 (s/serializer (:n-serializer opts))
                                                                 (s/serializer (:v-serializer opts)))
                     (.setColumnFamily cf)
                     (.setKeys (object-array pks))
                     (.setColumnNames (object-array scs))
                     (.setRange (:start opts) (:end opts) (:reversed opts) (:limit opts))))))

(defn get-rows
  "In keyspace ks, retrieve rows for pks within column family cf."
  [ks cf pks & o]
  (let [opts (extract-options o cf)]
    (execute-query (doto (HFactory/createMultigetSliceQuery ks
                                                            (s/serializer (first pks))
                                                            (s/serializer (:n-serializer opts))
                                                            (s/serializer (:v-serializer opts)))
                     (.setColumnFamily cf)
                     (.setKeys (object-array pks))
                     (.setRange (:start opts) (:end opts) (:reversed opts) (:limit opts))))))

(defn get-super-columns
  "In keyspace ks, for row pk, retrieve columns in c from super column sc."
  [ks cf pk sc c & o]
  (let [opts (extract-options o cf)]
    (execute-query (doto (HFactory/createSubSliceQuery ks
                                                       type-inferring
                                                       (s/serializer (:s-serializer opts))
                                                       (s/serializer (:n-serializer opts))
                                                       (s/serializer (:v-serializer opts)))
                     (.setColumnFamily cf)
                     (.setKey pk)
                     (.setSuperColumn sc)
                     (.setColumnNames (object-array c))))))

(defn get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  [ks cf pk c & o]
  (let [opts (extract-options o cf)
        vs (s/serializer (:v-serializer opts))
        ns (s/serializer (:n-serializer opts))]
    (if (< 2 (count c))
      (execute-query (doto (HFactory/createColumnQuery ks type-inferring ns vs)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setName c)))
      (execute-query (doto (HFactory/createSliceQuery ks type-inferring ns vs)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setColumnNames (object-array c)))))))

(defn get-counter-columns
  "Queries counter column values. c is a sequence of column names to
   retrieve the values for."
  [ks cf pk c & opts]
  (let [o (extract-options opts cf)
        ns (s/serializer (:n-serializer o))]
    (if (< 2 (count c))
      (execute-query (doto (HFactory/createCounterColumnQuery ks type-inferring ns)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setName (object-array c))))
      (execute-query (doto (HFactory/createCounterSliceQuery ks type-inferring ns)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setColumnNames (object-array c)))))))


(defn delete-columns
  "Deletes columns identified in cs for row pk."
  [ks cf pk cs]
  (let [mut (HFactory/createMutator ks type-inferring)]
    (doseq [c cs] (.addDeletion mut pk cf c type-inferring))
    (.execute mut)))

(defn delete-super-columns
  "Coll is a map of keys, super column names and column names

   Example: (delete-super-columns keyspace \"ColumnFamily\" {\"row-key\" {\"SuperCol\" [\"col-name\"]}})"
  [ks cf coll & o]
  (let [opts (extract-options o cf)
        mut (HFactory/createMutator ks type-inferring)]
    (doseq [[k nv] coll]
      (doseq [[sc-name v] nv]
        (.addSubDelete mut k cf (create-column sc-name
                                               (apply hash-map (interleave v v))
                                               :s-serializer (s/serializer (:s-serializer opts))
                                               :n-serializer (s/serializer (:n-serializer opts))
                                               :v-serializer (s/serializer (:v-serializer opts))))))
    (.execute mut)))

(defn delete-rows
  "Deletes all columns for rows identified in pks sequence."
  [ks cf pks]
  (let [mut (HFactory/createMutator ks type-inferring)]
    (doseq [k pks] (.addDeletion mut k cf))
    (.execute mut)))

(defnk count-columns
  "Counts number of columns for pk in column family cf. The method is not O(1).
   It takes all the columns from disk to calculate the answer. The only benefit
   of the method is that you do not need to pull all the columns over Thrift
   interface to count them."
  [ks pk cf :start nil :end nil :limit Integer/MAX_VALUE]
  (execute-query (doto (HFactory/createCountQuery ks
                                                  type-inferring
                                                  (s/serializer :bytes))
                   (.setKey pk)
                   (.setRange start end limit)
                   (.setColumnFamily cf))))


(defmacro defschema
  "Defines a schema for the named column family. The provided
   serializers will be used when operations are performed with
   the with-schemas macro.

   Example (defschema ColumnFamily [:n-serializer :string :v-serializer :string])"
  [cf-name ks]
  (let [name-str (as-str cf-name)]
    `(def ~cf-name {:name ~name-str
                    :serializers (apply hash-map ~ks)})))

(defn schemas-by-name
  [schemas]
  (->> schemas
       (map (fn [{:keys [name serializers]}]
              [name serializers]))
       (into {})))

(defmacro with-schemas
  "Binds schema information to *schemas*. Allows other get-xxx functions
   to re-use column family information and provide sensible default serializers
   without having to specify every time."
  [schemas & body]
  `(binding [*schemas* (schemas-by-name ~schemas)]
     ~@body))
