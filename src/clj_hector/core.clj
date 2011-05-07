(ns clj-hector.core
  (:require [clj-hector.serialize :as s])
  (:use [clojure.contrib.def :only (defnk)])
  (:import [java.io Closeable]
           [me.prettyprint.hector.api.mutation Mutator]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.cassandra.service CassandraHostConfigurator]
           [me.prettyprint.cassandra.serializers TypeInferringSerializer]))

;; following through sample usages on hector wiki
;; https://github.com/rantav/hector/wiki/User-Guide

(defn cluster
  "Connects to Cassandra cluster"
  ([^String cluster-name ^String host]
     (cluster cluster-name host 9160))
  ([^String cluster-name ^String host ^Integer port]
     (HFactory/getOrCreateCluster cluster-name
                                  (CassandraHostConfigurator. (str host ":" port)))))
(defn keyspace
  [cluster name]
  (HFactory/createKeyspace name cluster))

(defn- create-column
  "Creates columns or super-columns"
  ([n v]
     (let [s (TypeInferringSerializer/get)]
       (if (map? v)
         (let [cols (map (fn [[n v]] (create-column n v)) v)]
           (HFactory/createSuperColumn n cols s s s))
         (HFactory/createColumn n v s s)))))

(defn put-row
  "Stores values in columns in map m against row key pk"
  ([ks cf pk m]
     (let [^Mutator mut (HFactory/createMutator ks (TypeInferringSerializer/get))]
       (do (doseq [[k v] m]
             (.addInsertion mut pk cf (create-column k v)))
           (.execute mut)))))

(defnk get-super-rows
  [ks cf pks sc :s-serializer :bytes :n-serializer :bytes :v-serializer :bytes :start nil :end nil]
  (s/to-clojure (.execute (doto (HFactory/createMultigetSuperSliceQuery ks
                                                                        (s/serializer (first pks))
                                                                        (s/serializer s-serializer)
                                                                        (s/serializer n-serializer)
                                                                        (s/serializer v-serializer))
                            (.setColumnFamily cf)
                            (.setKeys (object-array pks))
                            (.setColumnNames (object-array sc))
                            (.setRange start end false Integer/MAX_VALUE)))))

(defnk get-rows
  "In keyspace ks, retrieve rows for pks within column family cf."
  [ks cf pks :n-serializer :bytes :v-serializer :bytes :start nil :end nil]
  (s/to-clojure (.execute (doto (HFactory/createMultigetSliceQuery ks
                                                                   (s/serializer (first pks))
                                                                   (s/serializer n-serializer)
                                                                   (s/serializer v-serializer))
                            (.setColumnFamily cf)
                            (.setKeys (object-array pks))
                            (.setRange start end false Integer/MAX_VALUE)))))

(defnk get-super-columns
  [ks cf pk sc c :s-serializer :bytes :n-serializer :bytes :v-serializer :bytes]
  (s/to-clojure (.execute (doto (HFactory/createSubSliceQuery ks
                                                              (TypeInferringSerializer/get)
                                                              (s/serializer s-serializer)
                                                              (s/serializer n-serializer)
                                                              (s/serializer v-serializer))
                            (.setColumnFamily cf)
                            (.setKey pk)
                            (.setSuperColumn sc)
                            (.setColumnNames (object-array c))))))

(defnk get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  [ks cf pk c :n-serializer :bytes :v-serializer :bytes]
  (let [s (TypeInferringSerializer/get)
        value-serializer (s/serializer v-serializer)
        name-serializer (s/serializer n-serializer)]
    (if (< 2 (count c))
      (s/to-clojure (.excute (doto (HFactory/createColumnQuery ks s name-serializer value-serializer)
                               (.setColumnFamily cf)
                               (.setKey pk)
                               (.setName c))))
      (s/to-clojure (.execute (doto (HFactory/createSliceQuery ks s name-serializer value-serializer)
                                (.setColumnFamily cf)
                                (.setKey pk)
                                (.setColumnNames (object-array c))))))))
(defn delete-columns
  [ks cf pk cs]
  (let [s (TypeInferringSerializer/get)
        mut (HFactory/createMutator ks s)]
    (doseq [c cs] (.addDeletion mut pk cf c s))
    (.execute mut)))

(defnk count-columns
  "Counts number of columns for pk in column family cf. The method is not O(1). It takes all the columns from disk to calculate the answer. The only benefit of the method is that you do not need to pull all the columns over Thrift interface to count them."
  [ks pk cf :start nil :end nil]
  (s/to-clojure (.execute (doto (HFactory/createCountQuery ks
                                                           (TypeInferringSerializer/get)
                                                           (s/serializer :bytes))
                            (.setKey pk)
                            (.setRange start end Integer/MAX_VALUE)
                            (.setColumnFamily cf)))))


