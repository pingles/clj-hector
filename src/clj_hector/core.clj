(ns clj-hector.core
  (:require [clj-hector.serialize :as s])
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
         (let [cols (map (fn [kv] (create-column (first kv) (last kv)))
                         v)]
           (HFactory/createSuperColumn n cols s s s))
         (HFactory/createColumn n v s s)))))

(defn put-row
  "Stores values in columns in map m against row key pk"
  ([ks cf pk m]
     (let [^Mutator mut (HFactory/createMutator ks (TypeInferringSerializer/get))]
       (do (doseq [kv m]
             (let [k (first kv) v (last kv)]
               (.addInsertion mut pk cf (create-column k v))))
           (.execute mut)))))

(defn get-rows
  "In keyspace ks, retrieve rows for pks within column family cf."
  ([ks cf pks]
     (get-rows ks cf pks {}))
  ([ks cf pks opts]
     (s/to-clojure (let [value-serializer (s/serializer (or (:v-serializer opts) :bytes))
                         name-serializer (s/serializer (or (:n-serializer opts) :bytes))]
                     (.execute (doto (HFactory/createMultigetSliceQuery ks
                                                                  (s/serializer (first pks))
                                                                  name-serializer
                                                                  value-serializer)
                           (.setColumnFamily cf)
                           (.setKeys (object-array pks))
                           (.setRange (:start opts) (:end opts) false Integer/MAX_VALUE))))))
  ([ks cf pks sc opts]
     (s/to-clojure (let [s-serializer (s/serializer (or (:s-serializer opts) :bytes))
                         n-serializer (s/serializer (or (:n-serializer opts) :bytes))
                         v-serializer (s/serializer (or (:v-serializer opts) :bytes))]
                     (.execute (doto (HFactory/createMultigetSuperSliceQuery ks
                                                                             (s/serializer (first pks))
                                                                             s-serializer
                                                                             n-serializer
                                                                             v-serializer)
                                 (.setColumnFamily cf)
                                 (.setKeys (object-array pks))
                                 (.setColumnNames (object-array (seq sc)))
                                 (.setRange (:start opts) (:end opts) false Integer/MAX_VALUE)))))))

(defn delete-columns
  [ks cf pk cs]
  (let [s (TypeInferringSerializer/get)
        mut (HFactory/createMutator ks s)]
    (doseq [c cs] (.addDeletion mut pk cf c s))
    (.execute mut)))

(defn count-columns
  "Counts number of columns for pk in column family cf. The method is not O(1). It takes all the columns from disk to calculate the answer. The only benefit of the method is that you do not need to pull all the columns over Thrift interface to count them."
  [ks pk cf & {:keys [n-serializer start end] :or {n-serializer :bytes}}]
  (let [name-serializer (s/serializer n-serializer)]
    (s/to-clojure (.execute (doto (HFactory/createCountQuery ks
                                                             (TypeInferringSerializer/get)
                                                             name-serializer)
                              (.setKey pk)
                              (.setRange start end Integer/MAX_VALUE)
                              (.setColumnFamily cf))))))

(defn get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  ([ks cf pk c]
     (get-columns ks cf pk c {}))
  ([ks cf pk c opts]
     (let [s (TypeInferringSerializer/get)
           value-serializer (s/serializer (or (:v-serializer opts) :bytes))
           name-serializer (s/serializer (or (:n-serializer opts) :bytes))]
       (if (< 2 (count c))
         (s/to-clojure (.excute (doto (HFactory/createColumnQuery ks s name-serializer value-serializer)
                                  (.setColumnFamily cf)
                                  (.setKey pk)
                                  (.setName c))))
         (s/to-clojure (.execute (doto (HFactory/createSliceQuery ks s name-serializer value-serializer)
                                   (.setColumnFamily cf)
                                   (.setKey pk)
                                   (.setColumnNames (object-array (seq c)))))))))
  ([ks cf pk sc c opts]
     (let [s (s/serializer (or (:s-serializer opts)
                               :bytes))
           n (s/serializer (or (:n-serializer opts)
                               :bytes))
           v (s/serializer (or (:v-serializer opts)
                               :bytes))]
       (s/to-clojure (.execute (doto (HFactory/createSubSliceQuery ks (TypeInferringSerializer/get) s n v)
                           (.setColumnFamily cf)
                           (.setKey pk)
                           (.setSuperColumn sc)
                           (.setColumnNames (object-array (seq c)))))))))

