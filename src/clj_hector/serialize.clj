(ns clj-hector.serialize
  (:import [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer TypeInferringSerializer BytesArraySerializer SerializerTypeInferer UUIDSerializer BigIntegerSerializer BooleanSerializer DateSerializer ObjectSerializer AsciiSerializer ByteBufferSerializer FloatSerializer CharSerializer DoubleSerializer ShortSerializer]
           [me.prettyprint.cassandra.model QueryResultImpl HColumnImpl ColumnSliceImpl RowImpl RowsImpl SuperRowImpl SuperRowsImpl HSuperColumnImpl]
           [me.prettyprint.hector.api.ddl KeyspaceDefinition ColumnFamilyDefinition ColumnDefinition]
           [me.prettyprint.hector.api Serializer]
           [java.nio ByteBuffer]))

(defprotocol ToClojure
  (to-clojure [_] "Convert hector types to Clojure data structures."))

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
                    :bytes (BytesArraySerializer/get)
                    :uuid (UUIDSerializer/get)
                    :bigint (BigIntegerSerializer/get)
                    :bool (BooleanSerializer/get)
                    :date (DateSerializer/get)
                    :object (ObjectSerializer/get)
                    :ascii (AsciiSerializer/get)
                    :byte-buffer (ByteBufferSerializer/get)
                    :char (CharSerializer/get)
                    :double (DoubleSerializer/get)
                    :float (FloatSerializer/get)
                    :short (ShortSerializer/get)})

(defn serializer
  "Returns an instance of the specified serializer.

   Argument: either a) instance of Serializer.
                    b) a keyword for one of the supported serializers.

   Supported serializers: :integer, :string, :long, :bytes, :uuid
   :bigint, :bool, :date, :object, :ascii, :byte-buffer, :char, :double
   :float, :short."
  [s]
  (cond (keyword? x) (x *serializers*)
        (instance? Serializer x) x
        :else (SerializerTypeInferer/getSerializer x)))
