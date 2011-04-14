(ns clj-hector.serialize
  (:import [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer TypeInferringSerializer BytesArraySerializer SerializerTypeInferer]
           [me.prettyprint.cassandra.model QueryResultImpl HColumnImpl ColumnSliceImpl RowImpl RowsImpl SuperRowImpl SuperRowsImpl HSuperColumnImpl]))

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

(defn serializer
  "Returns serialiser based on type of item"
  [x]
  (if (keyword? x)
    (x *serializers*)
    (SerializerTypeInferer/getSerializer x)))
