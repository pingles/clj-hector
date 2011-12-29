(ns clj-hector.serialize
  ^{:author "Paul Ingles"
    :description "Utilities for serializing and deserializing Clojure and Hector types."}
  (:import [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer TypeInferringSerializer BytesArraySerializer SerializerTypeInferer UUIDSerializer BigIntegerSerializer BooleanSerializer DateSerializer ObjectSerializer AsciiSerializer ByteBufferSerializer FloatSerializer CharSerializer DoubleSerializer ShortSerializer CompositeSerializer DynamicCompositeSerializer]
           [me.prettyprint.cassandra.model QueryResultImpl HColumnImpl ColumnSliceImpl RowImpl RowsImpl SuperRowImpl SuperRowsImpl HSuperColumnImpl CounterSliceImpl HCounterColumnImpl CounterSuperSliceImpl HCounterSuperColumnImpl CounterRowsImpl CounterRowImpl]
           [me.prettyprint.hector.api.ddl KeyspaceDefinition ColumnFamilyDefinition ColumnDefinition]
           [me.prettyprint.hector.api.beans AbstractComposite AbstractComposite$Component]
           [me.prettyprint.hector.api Serializer]
           [java.nio ByteBuffer]))

(def serializers {:integer (IntegerSerializer/get)
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
                  :short (ShortSerializer/get)
                  :dynamic-composite (new DynamicCompositeSerializer)
                  :composite (new CompositeSerializer)
                  :type-inferring (TypeInferringSerializer/get)})

(defn serializer
  "Returns an instance of the specified serializer.

   Argument: either a) instance of Serializer.
                    b) a keyword for one of the supported serializers.
                    c) any object.

   If an object is passed the relevant serializer will be determined by
   Hector's SerializerTypeInferer. This can be useful when serializing
   strings or other types where serializers can be determined automatically.

   Supported serializers: :integer, :string, :long, :bytes, :uuid
   :bigint, :bool, :date, :object, :ascii, :byte-buffer, :char, :double
   :float, :short."
  [x]
  (cond (keyword? x) (x serializers)
        (instance? Serializer x) x
        :else (SerializerTypeInferer/getSerializer x)))

(defn- deserialize-composite
  "Given a composite and a list of deserializers deserialize the
   component values of the composite returning a vector of values.
   Unfortunatly this is required due to a limition of the Composite
   implementation in Hector.  this isn't neccessary for
   DynamicComposite."

  [composite deserializers]
  (into []
        (map (fn [component deserializer]
               (.fromByteBuffer (serializer deserializer) (.getBytes component)))
             (.getComponents composite)
             deserializers)))


(defprotocol ToClojure
  (to-clojure [_ _] "Convert hector types to Clojure data structures."))

(defn- tc-swap
  [opts s]
  (to-clojure s opts))

(extend-protocol ToClojure
  ColumnDefinition
  (to-clojure [c _] {:name (.getName c)
                       :index (.getIndexName c)
                       :index-type (.getIndexType c)
                       :validation-class (.getValidationClass c)})

  ColumnFamilyDefinition
  (to-clojure [c opts] {:name (.getName c)
                       :comment (.getComment c)
                       :column-type (.getColumnType c)
                       :comparator-type (.getComparatorType c)
                       :sub-comparator-type (.getSubComparatorType c)
                       :columns (map (partial tc-swap opts) (.getColumnMetadata c))})

  KeyspaceDefinition
  (to-clojure [k opts] {(.getName k) {:strategy (.getStrategyClass k)
                                     :replication (.getReplicationFactor k)
                                     :column-families (map (partial tc-swap opts) (.getCfDefs k))}})
  CounterRowsImpl
  (to-clojure [s opts]
    (into {} (partial tc-swap opts) (iterator-seq (.iterator s))))
  CounterRowImpl
  (to-clojure [s opts]
    {(.getKey s) (to-clojure (.getColumnSlice s) opts)})
  SuperRowsImpl
  (to-clojure [s opts]
    (map (partial tc-swap opts) (iterator-seq (.iterator s))))
  SuperRowImpl
  (to-clojure [s opts]
    {(.getKey s) (map (partial tc-swap opts) (seq (.. s getSuperSlice getSuperColumns)))})
  HSuperColumnImpl
  (to-clojure [s opts]
    {(.getName s) (into {} (map (partial tc-swap opts) (.getColumns s)))})
  RowsImpl
  (to-clojure [s opts]
    (map (partial tc-swap opts) (iterator-seq (.iterator s))))
  RowImpl
  (to-clojure [s opts]
    {(.getKey s) (to-clojure (.getColumnSlice s) opts)})
  ColumnSliceImpl
  (to-clojure [s opts]
    (into (hash-map) (for [c (.getColumns s)] (to-clojure c opts))))
  HColumnImpl
  (to-clojure [s opts]
    {(let [col (.getName s)] (if (instance? AbstractComposite col)
                               (to-clojure col opts) col)) (.getValue s)})
  HCounterColumnImpl
  (to-clojure [s opts]
    {(.getName s) (.getValue s)})
  CounterSuperSliceImpl
  (to-clojure [s opts]
    (into {} (map (partial tc-swap opts) (.getSuperColumns s))))
  HCounterSuperColumnImpl
  (to-clojure [s opts]
    {(.getName s) (into {} (map (partial tc-swap opts) (.getColumns s)))})
  CounterSliceImpl
  (to-clojure [s opts]
    (into {} (map (partial tc-swap opts) (.getColumns s))))
  Integer
  (to-clojure [s _]
    {:count s})
  AbstractComposite
  (to-clojure [s opts]
    (let [serializers (:c-serializer opts)]
      (if serializers
        (deserialize-composite s serializers)
        (into [] (map #(.getValue %1) (.getComponents s))))))
  QueryResultImpl
  (to-clojure [s opts]
    (with-meta (to-clojure (.get s) opts) {:exec_us (.getExecutionTimeMicro s)
                                           :host (.getHostUsed s)})))
