(ns clj-hector.core
  (:import [java.io Closeable]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.cassandra.service CassandraHostConfigurator]
           [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer TypeInferringSerializer BytesArraySerializer SerializerTypeInferer]
           [me.prettyprint.cassandra.model QueryResultImpl HColumnImpl ColumnSliceImpl RowImpl RowsImpl SuperRowImpl SuperRowsImpl HSuperColumnImpl]))

;; work in progress; following through sample usages on hector wiki
;; https://github.com/rantav/hector/wiki/User-Guide

(defn closeable-cluster
  [cluster]
  (proxy [Cluster Closeable] []
    (close []
           (.. cluster getConnectionManager shutdown))))

(defn cluster
  "Connects to Cassandra cluster"
  ([cluster-name host]
     (cluster cluster-name host 9160))
  ([cluster-name host port]
     (HFactory/getOrCreateCluster cluster-name
                                  (CassandraHostConfigurator. (str host ":" port)))))
(defn keyspace
  [cluster name]
  (HFactory/createKeyspace name cluster))

(defprotocol ToClojure
  (to-clojure [x] "Convert hector types to Clojure data structures"))

(extend-protocol ToClojure
  SuperRowsImpl
  (to-clojure [s]
              (map to-clojure (iterator-seq (.iterator s))))

  SuperRowImpl
  (to-clojure [s]
              {:key (.getKey s)
               :super-columns (map to-clojure (seq (.. s getSuperSlice getSuperColumns)))})

  HSuperColumnImpl
  (to-clojure [s]
              {:name (.getName s)
               :columns (into (sorted-map) (for [c (.getColumns s)] (to-clojure c)))})

  RowsImpl
  (to-clojure [s]
              (map to-clojure (iterator-seq (.iterator s))))
  RowImpl
  (to-clojure [s]
              {:key (.getKey s)
               :columns (to-clojure (.getColumnSlice s))})
  
  ColumnSliceImpl
  (to-clojure [s]
              (into (sorted-map) (for [c (.getColumns s)] (to-clojure c))))
  HColumnImpl
  (to-clojure [s]
              {(.getName s) (.getValue s)})

  Integer
  (to-clojure [s]
              {:count s})

  QueryResultImpl
  (to-clojure [s]
              (with-meta (to-clojure (.get s)) {:exec_us (.getExecutionTimeMicro s)
                                                :host (.getHostUsed s)})))

(def *serializers* {:integer (IntegerSerializer/get)
                    :string (StringSerializer/get)
                    :long (LongSerializer/get)
                    :bytes (BytesArraySerializer/get)})

(defn- serializer
  "Returns serialiser based on type of item"
  [x]
  (if (keyword? x)
    (x *serializers*)
    (SerializerTypeInferer/getSerializer x)))

(defn- create-column
  "Creates columns or super-columns"
  ([k v]
     (let [s (TypeInferringSerializer/get)]
       (HFactory/createColumn k v s s)))
  ([sc k v]
     (HFactory/createSuperColumn sc
                                 (list (create-column k v))
                                 (TypeInferringSerializer/get)
                                 (TypeInferringSerializer/get)
                                 (TypeInferringSerializer/get))))

;; TODO: This could be improved when inserting multiple n/v columns into a supercolumn
(defn put-row
  "Stores values in columns in map m against row key pk"
  ([ks cf pk m]
     (put-row ks cf pk nil m))
  ([ks cf pk sc m]
     (let [mut (HFactory/createMutator ks (TypeInferringSerializer/get))]
       (do (doseq [kv m]
             (let [k (first kv) v (last kv)
                   col (if (nil? sc)
                         (create-column k v)
                         (create-column sc k v))]
               (.addInsertion mut pk cf col)))
           (.execute mut)))))

(defn get-rows
  "In keyspace ks, retrieve rows for pks within column family cf."
  ([ks cf pks]
     (get-rows ks cf pks {}))
  ([ks cf pks opts]
     (to-clojure (let [value-serializer (serializer (or (:v-serializer opts) :bytes))
                       name-serializer (serializer (or (:n-serializer opts) :bytes))]
                   (.. (doto (HFactory/createMultigetSliceQuery ks
                                                                (serializer (first pks))
                                                                name-serializer
                                                                value-serializer)
                         (.setColumnFamily cf)
                         (.setKeys (object-array pks))
                         (.setRange (:start opts) (:end opts) false Integer/MAX_VALUE))
                       execute))))
  ([ks cf pks sc opts]
     (to-clojure (let [s-serializer (serializer (or (:s-serializer opts) :bytes))
                       n-serializer (serializer (or (:n-serializer opts) :bytes))
                       v-serializer (serializer (or (:v-serializer opts) :bytes))]
                   (.. (doto (HFactory/createMultigetSuperSliceQuery ks
                                                                     (serializer (first pks))
                                                                     s-serializer
                                                                     n-serializer
                                                                     v-serializer)
                         (.setColumnFamily cf)
                         (.setKeys (object-array pks))
                         (.setColumnNames (object-array (seq sc)))
                         (.setRange (:start opts) (:end opts) false Integer/MAX_VALUE))
                       execute)))))

(defn delete-columns
  [ks cf pk cs]
  (let [s (TypeInferringSerializer/get)
        mut (HFactory/createMutator ks s)]
    (doseq [c cs] (.addDeletion mut pk cf c s))
    (.execute mut)))

(defn count-columns
  "Counts number of columns for pk in column family cf. The method is not O(1). It takes all the columns from disk to calculate the answer. The only benefit of the method is that you do not need to pull all the columns over Thrift interface to count them."
  [ks pk cf & opts]
  (let [name-serializer (serializer (or (:n-serializer opts) :bytes))]
    (to-clojure (.. (doto (HFactory/createCountQuery ks
                                                     (TypeInferringSerializer/get)
                                                     name-serializer)
                      (.setKey pk)
                      (.setRange (:start opts) (:end opts) Integer/MAX_VALUE)
                      (.setColumnFamily cf))
                    execute))))

(defn get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  ([ks cf pk c]
     (get-columns ks cf pk c {}))
  ([ks cf pk c opts]
     (let [s (TypeInferringSerializer/get)
           value-serializer (serializer (or (:v-serializer opts) :bytes))
           name-serializer (serializer (or (:n-serializer opts) :bytes))]
       (if (< 2 (count c))
         (to-clojure (.. (doto (HFactory/createColumnQuery ks s name-serializer value-serializer)
                           (.setColumnFamily cf)
                           (.setKey pk)
                           (.setName c))
                         execute))
         (to-clojure (.. (doto (HFactory/createSliceQuery ks s name-serializer value-serializer)
                           (.setColumnFamily cf)
                           (.setKey pk)
                           (.setColumnNames (object-array c)))
                         execute))))))

