(ns clj-hector.core
  (:import [java.io Closeable]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.cassandra.service CassandraHostConfigurator]
           [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer]
           [me.prettyprint.cassandra.model QueryResultImpl HColumnImpl ColumnSliceImpl RowImpl RowsImpl]))

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
  RowsImpl
  (to-clojure [s]
              (map to-clojure (iterator-seq (.iterator s))))
  RowImpl
  (to-clojure [s]
              {:key (.getKey s)
               :columns (to-clojure (.getColumnSlice s))})
  
  ColumnSliceImpl
  (to-clojure [s]
              (into {}
                    (for [c (.getColumns s)]
                      (to-clojure c))))
  HColumnImpl
  (to-clojure [s]
              {(.getName s) (.getValue s)})

  QueryResultImpl
  (to-clojure [s]
              (with-meta (to-clojure (.get s)) {:exec_us (.getExecutionTimeMicro s)
                                                :host (.getHostUsed s)})))

(def *serializers* {:integer (IntegerSerializer/get)
                    :string (StringSerializer/get)
                    :long (LongSerializer/get)})

(defn serializer
  "Returns serialiser based on type of item"
  [t]
  (cond (instance? Integer t) (:integer *serializers*)
        (instance? Long t) (:long *serializers*)
        :else (:string *serializers*)))

(def *string-serializer* (StringSerializer/get))

(defn- create-column
  [k v]
  (HFactory/createColumn k v (serializer k) (serializer v)))

(defn put-row
  "Stores values in columns in map m against row key pk"
  [ks cf pk m]
  (let [mut (HFactory/createMutator ks (serializer pk))]
    (if (= 1 (count (keys m)))
      (let [k (first (keys m))
            v (first (vals m))]
        (.insert mut pk cf (create-column k v)))
      (do (doseq [kv m]
            (let [k (first kv) v (last kv)]
              (.addInsertion mut pk cf (create-column k v))))
          (.execute mut)))))

(defn get-rows
  "In keyspace ks, retrieve rows for pks within column family cf."
  ([ks cf pks]
     (get-rows ks cf pks {:v-serializer :string}))
  ([ks cf pks opts]
     (to-clojure (let [value-serializer (*serializers* (:v-serializer opts))]
                   (.. (doto (HFactory/createMultigetSliceQuery ks
                                                                (serializer (first pks))
                                                                *string-serializer*
                                                                value-serializer)
                         (.setColumnFamily cf)
                         (. setKeys (object-array pks))
                         (.setRange "" "" false Integer/MAX_VALUE))
                       execute)))))

(defn delete-columns
  [ks cf pk cs]
  (let [mut (HFactory/createMutator ks (serializer pk))]
    (doseq [c cs] (.addDeletion mut pk cf c (serializer pk)))
    (.execute mut)))

(defn get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  ([ks cf pk c]
     (get-columns ks cf pk c {:v-serializer :string}))
  ([ks cf pk c opts]
     (let [value-serializer (*serializers* (:v-serializer opts))] 
       (if (< 2 (count c))
         (to-clojure (.. (doto (HFactory/createColumnQuery ks
                                                           (serializer pk)
                                                           *string-serializer*
                                                           value-serializer)
                           (.setColumnFamily cf)
                           (.setKey pk)
                           (.setName c))
                         execute))
         (to-clojure (.. (doto (HFactory/createSliceQuery ks
                                                          (serializer pk)
                                                          *string-serializer*
                                                          value-serializer)
                           (.setColumnFamily cf)
                           (.setKey pk)
                           (. setColumnNames (object-array c)))
                         execute))))))

