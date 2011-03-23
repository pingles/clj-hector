(ns clj-hector.core
  (:import [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.cassandra.service CassandraHostConfigurator]
           [me.prettyprint.cassandra.serializers StringSerializer]
           [me.prettyprint.cassandra.model HColumnImpl ColumnSliceImpl RowImpl RowsImpl]))

;; work in progress; following through sample usages on hector wiki
;; https://github.com/rantav/hector/wiki/User-Guide

(defn cluster
  "Connects to Cassandra cluster"
  [cluster-name host port]
  (HFactory/getOrCreateCluster cluster-name
                               (CassandraHostConfigurator. (str host ":" port))))
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
              {(.getName s) (.getValue s)}))

(def *string-serializer* (StringSerializer/get))

(defn get-rows
  "In keyspace ks, retrieve rows for pks within column family cf"
  [ks cf pks]
  (to-clojure (.. (doto (HFactory/createMultigetSliceQuery ks
                                                           *string-serializer*
                                                           *string-serializer*
                                                           *string-serializer*)
                    (.setColumnFamily cf)
                    (. setKeys pks)
                    (.setRange "" "" false, 3))
                  execute get)))

(defn get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  [ks cf pk c]
  (if (< 2 (count c))
    (to-clojure (.. (doto (HFactory/createStringColumnQuery ks)
                      (.setColumnFamily cf)
                      (.setKey pk)
                      (.setName c))
                    execute get))
    (to-clojure (.. (doto (HFactory/createSliceQuery ks
                                                     *string-serializer*
                                                     *string-serializer*
                                                     *string-serializer*)
                      (.setColumnFamily cf)
                      (.setKey pk)
                      (. setColumnNames (object-array c)))
                    execute get))))

