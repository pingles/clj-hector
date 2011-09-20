(ns clj-hector.serialize
  (:import [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer TypeInferringSerializer BytesArraySerializer SerializerTypeInferer]
           [me.prettyprint.cassandra.model QueryResultImpl HColumnImpl ColumnSliceImpl RowImpl RowsImpl SuperRowImpl SuperRowsImpl HSuperColumnImpl]
           [me.prettyprint.hector.api.ddl KeyspaceDefinition ColumnFamilyDefinition ColumnDefinition]))

(defprotocol ToClojure
  (to-clojure [x] "Convert hector types to Clojure data structures"))

(extend-protocol ToClojure
  ColumnDefinition
  (to-clojure [c] {:name (.getName c)
                   :index (.getIndexName c)
                   :index-type (.getIndexType c)
                   :validation-class (.getValidationClass c)})

  ColumnFamilyDefinition
  (to-clojure [c] {:name (.getName c)
                   :comment (.getComment c)
                   :column-type (.getColumnType c)
                   :comparator-type (.getComparatorType c)
                   :sub-comparator-type (.getSubComparatorType c)
                   :columns (map to-clojure (.getColumnMetadata c))})
  
  KeyspaceDefinition
  (to-clojure [k] {(.getName k) {:strategy (.getStrategyClass k)
                                 :replication (.getReplicationFactor k)
                                 :column-families (map to-clojure (.getCfDefs k))}})
  SuperRowsImpl
  (to-clojure [s]
              (map to-clojure (iterator-seq (.iterator s))))
  SuperRowImpl
  (to-clojure [s]
              {(.getKey s) (map to-clojure (seq (.. s getSuperSlice getSuperColumns)))})
  HSuperColumnImpl
  (to-clojure [s]
              {(.getName s) (into (hash-map) (for [c (.getColumns s)] (to-clojure c)))})
  RowsImpl
  (to-clojure [s]
              (map to-clojure (iterator-seq (.iterator s))))
  RowImpl
  (to-clojure [s]
              {(.getKey s) (to-clojure (.getColumnSlice s))})
  ColumnSliceImpl
  (to-clojure [s]
              (into (hash-map) (for [c (.getColumns s)] (to-clojure c))))
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

(defn serializer
  "Returns serialiser based on type of item"
  [x]
  (if (keyword? x)
    (x *serializers*)
    (SerializerTypeInferer/getSerializer x)))
