(ns ^{:author "Paul Ingles"
      :doc "Hector-based Cassandra client"}
  clj-hector.core
  (:require [clj-hector.serialize :as s])
  (:require [clj-hector.ddl :as ddl])
  (:require [clj-hector.consistency :as c])
  (:import [java.io Closeable]
           [me.prettyprint.hector.api.mutation Mutator]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.hector.api.query Query]
           [me.prettyprint.cassandra.model CqlQuery]
           [me.prettyprint.hector.api.beans Composite DynamicComposite]
           [me.prettyprint.cassandra.service CassandraHostConfigurator]
           [me.prettyprint.cassandra.serializers TypeInferringSerializer]))

(def ^:dynamic *schemas* {})

(def type-inferring (TypeInferringSerializer/get))

(defn cluster
  "Connects to Cassandra cluster"
  ([cluster-name host]
     (cluster cluster-name host 9160))
  ([cluster-name host port]
     (cluster cluster-name host port (CassandraHostConfigurator. )))
  ([cluster-name host port configurator]
     (cluster cluster-name host port configurator {}))
  ([cluster-name host port configurator credentials]
     (HFactory/getOrCreateCluster cluster-name
                                  (doto configurator (.setHosts (str host ":" port)))
                                  credentials)))

(defn shutdown-cluster! [c] (HFactory/shutdownCluster c))

(defn cluster-name [c] (.describeClusterName c))

(defn partitioner [c] (.describePartitioner c))

(defn keyspace
  "Connects the client to the specified Keyspace. All other interactions
   with Cassandra are performed against this keyspace.

   cluster is the hector cluster object, name is the string name of the keyspace

   An optional consistency map can be passed indicating the desired consistency levels
   for each cf/operation type combination. The default is a consistency level of ONE
   and a default across all cfs can be defined/overridden by using the keyspace name '*'.
  "
  ([cluster name] (keyspace cluster name {"*" {:read :one :write :one}}))
  ([cluster name consistency-map]
   (HFactory/createKeyspace name cluster (c/policy consistency-map))))

(defn- schema-options
  [opts column-family]
  (let [defaults {:type :standard
                  :counter false
                  :ttl nil
                  :s-serializer :bytes
                  :k-serializer :bytes
                  :n-serializer :bytes
                  :v-serializer :bytes
                  :c-serializer nil}]
   (merge defaults (apply hash-map opts) (get *schemas* column-family))))

(defn- query-options
  [opts]
  (let [defaults {:start nil
                  :end nil
                  :reversed false
                  :row-limit 10
                  :limit Integer/MAX_VALUE}]
    (merge defaults (apply hash-map opts))))

(defn extract-options
  [opts cf]
  (merge (query-options opts) (schema-options opts cf)))

(def serializer-keys [:n-serializer :v-serializer :s-serializer])

(defn convert-serializers [opts]
  (reduce (fn [m [k v]] (assoc m k (s/serializer v)))
          opts
          (select-keys opts serializer-keys)))

(defn- super? [opts]
  (= (opts :type :standard) :super))

(defn- counter? [opts]
  (get opts :counter false))

(defn- populate-composite
  "populate the component values of a DynamicComposite or Composite"
  [composite components]
  (doseq [c components]
    (if (map? c)
      (let [opts (merge {:n-serializer :bytes
                         :comparator :bytes
                         :equality :equal} c)]
        (.addComponent composite
                       (:value opts)
                       (s/serializer (:n-serializer opts))
                       (.getTypeName (ddl/comparator-types (:comparator opts)))
                       (ddl/component-equality-type (:equality opts))))
      (.add composite -1 c)))
  composite)

(defn create-composite
  "Given a list create a Composite

  Supply a list of hashes to specify Component options for each element in the composite

  ex: [\"col\" \"name\"]
  ex: [{:value \"col\" :n-serializer :string :comparator :utf-8 :equality :equal}
       {:value 2 :n-serializer :string :comparator :integer :equality :less_than_equal}]"
  [& components]
  (let [^Composite composite (Composite. )]
    (populate-composite composite components)))

(defn create-dynamic-composite
  "Given a list create a DynamicComposite

  Supply a list of hashes to specify Component options for each element in the composite

  ex: [\"col\" \"name\"]
  ex: [{:value \"col\" :n-serializer :string :comparator :utf-8 :equality :equal}
       {:value 2 :n-serializer :string :comparator :integer :equality :less_than_equal}]"
  [& components]
  (let [^DynamicComposite composite (DynamicComposite. )]
    (populate-composite composite components)))

(defn- create-column
  "Creates Column and SuperColumns.

   Serializers for the super column name, column name, and column value default to an instance of TypeInferringSerializer.

   Examples: (create-column \"name\" \"a value\")  (create-column \"super column name\" {\"name\" \"value\"})"
  [name value options]
  (let [opts (convert-serializers options)
        n-serializer (opts :n-serializer)
        s-serializer (opts :s-serializer)
        v-serializer (opts :v-serializer)
        ttl (opts :ttl)]
    (if (super? opts)
      (let [cols (map (fn [[n v]] (create-column n v (dissoc options :type))) value)]
        (if (counter? opts)
          (HFactory/createCounterSuperColumn name cols s-serializer n-serializer)
          (HFactory/createSuperColumn name cols s-serializer n-serializer v-serializer)))
      (if (counter? opts)
        (HFactory/createCounterColumn name value n-serializer)
        (if ttl
          (HFactory/createColumn name value (int ttl) n-serializer v-serializer)
          (HFactory/createColumn name value n-serializer v-serializer))))))

(defn batch-put
  "Add multiple rows before executing the put operation. Rows are expressed as
   a map, i.e. {<row-pk> {<col-k> <col-v>, ... }, <row-pk> {...}, ...}

   NOTE: You will need to experiment to find the right batch size for your
   specific use case. While a larger batch may improve performance, overly
   large batches are discouraged. When a batch mutation fails, the entire
   request must be retried. Additionally, loading very large batches into
   memory can cause problems on the server."
  [ks cf rows & {:as opts}]
  (let [^Mutator mut (HFactory/createMutator ks type-inferring)
        defaults (merge {:n-serializer :type-inferring
                         :v-serializer :type-inferring
                         :s-serializer :type-inferring}
                        opts)
        opts (extract-options (apply concat (seq defaults)) cf)
        mut-add (if (counter? opts)
                  (fn [pk cf n v]
                    (.addCounter mut pk cf (create-column n v opts)))
                  (fn [pk cf k v]
                    (.addInsertion mut pk cf (create-column k v opts))))]
    (doseq [[pk col-map] rows
            [k v]        col-map]
      (mut-add pk cf k v))
    (.execute mut)))

(defn put
  "Stores values in columns in map m against row key pk"
  [ks cf pk col-map & opts]
  (apply batch-put ks cf {pk col-map} opts))

(defn create-counter-column
  [name value & {:keys [n-serializer v-serializer s-serializer]
                 :or {n-serializer type-inferring
                      v-serializer type-inferring
                      s-serializer type-inferring}}]
  (if (map? value)
    (let [cols (map (fn [[n v]] (create-counter-column n v :n-serializer n-serializer :v-serializer v-serializer)) value)]
      (HFactory/createCounterSuperColumn name cols s-serializer n-serializer))
    (HFactory/createCounterColumn name value n-serializer)))

(defn- execute-query [^Query query & [opts]]
  (s/to-clojure (.execute query) opts))

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
                     (.setKeys (into-array pks))
                     (.setColumnNames (into-array scs))
                     (.setRange (:start opts) (:end opts) (:reversed opts) (:limit opts)))
                   opts)))

(defn get-rows
  "In keyspace ks, retrieve rows for pks within column family cf."
  [ks cf pks & o]
  (let [opts (extract-options o cf)]
    (execute-query (doto (HFactory/createMultigetSliceQuery ks
                                                            (s/serializer (first pks))
                                                            (s/serializer (:n-serializer opts))
                                                            (s/serializer (:v-serializer opts)))
                     (.setColumnFamily cf)
                     (.setKeys (into-array pks))
                     (.setRange (:start opts) (:end opts) (:reversed opts) (:limit opts)))
                   opts)))


(defn get-row-slice
  "In keyspace ks, retrieve row range within column family cf."
  [ks cf f l & o]
  (let [opts (extract-options o cf)]
    (execute-query (doto (HFactory/createRangeSlicesQuery ks
                                                          (s/serializer (:k-serializer opts))
                                                          (s/serializer (:n-serializer opts))
                                                          (s/serializer (:v-serializer opts)))
                     (.setColumnFamily cf)
                     (.setKeys f l)
                     (.setRange (:start opts) (:end opts) (:reversed opts) (:limit opts))
                     (.setRowCount (Integer. (:row-limit opts))))
                   opts)))

(defn row-sequence
  "In keyspace ks, retrieve row range within column family cf. Returned as a lazy sequence."
  [ks cf f l & o]
  (let [opts (extract-options o cf)
        rows (apply get-row-slice ks cf f l o)
        next-page (last (mapcat keys rows))]

    (if-not (= next-page f)
      (concat (if-not (:inclusive opts) (remove #(= f (first (keys %1))) rows) rows)
              (lazy-seq
               (apply row-sequence ks cf next-page l (flatten (into [] opts))))))))

(defn get-rows-cql-query
  "In keyspace ks, retrieve rows for pks within column family cf."
  [ks query & o]
  (let [opts (extract-options o nil)]
    (execute-query (doto (CqlQuery. ks,
                                    (s/serializer (:k-serializer opts))
                                    (s/serializer (:n-serializer opts))
                                    (s/serializer (:v-serializer opts)))
                     (.setSuppressKeyInColumns true)
                     (.setQuery query))
                   opts)))

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
                     (.setColumnNames (into-array c)))
                   opts)))

(defn get-column-range
  "In keyspace ks, retrieve columns between start and end from column family cf."
  [ks cf pk start end & o]
  (let [opts (extract-options o cf)
        vs (s/serializer (:v-serializer opts))
        ns (s/serializer (:n-serializer opts))]
    (execute-query (doto (HFactory/createSliceQuery ks type-inferring ns vs)
                     (.setColumnFamily cf)
                     (.setKey pk)
                     (.setRange start end (:reversed opts) (:limit opts)))
                   opts)))

(defn get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  [ks cf pk c & o]
  (let [opts (extract-options o cf)
        vs (s/serializer (:v-serializer opts))
        ns (s/serializer (:n-serializer opts))]
    (if (coll? c)
      (execute-query (doto (HFactory/createSliceQuery ks type-inferring ns vs)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setColumnNames (into-array c)))
                     opts)
      (execute-query (doto (HFactory/createColumnQuery ks type-inferring ns vs)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setName c))
                     opts))))

(defn get-counter-columns
  "Queries counter column values. c is a sequence of column names to
   retrieve the values for."
  [ks cf pk c & opts]
  (let [o (extract-options opts cf)
        ns (s/serializer (:n-serializer o))]
    (if (coll? c)
      (execute-query (doto (HFactory/createCounterSliceQuery ks type-inferring ns)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setColumnNames (into-array c)))
                     opts)
      (execute-query (doto (HFactory/createCounterColumnQuery ks type-inferring ns)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setName (into-array c)))
                     opts))))

(defn get-counter-rows
  "Load data for specified keys and columns"
  [ks cf pks cs & o]
  (let [opts (extract-options o cf)
        ns (s/serializer (:n-serializer opts))
        kser (s/serializer (:k-serializer opts))]
    (execute-query (doto (HFactory/createMultigetSliceCounterQuery ks kser ns)
                     (.setKeys pks)
                     (.setColumnFamily cf)
                     (.setColumnNames (into-array cs)))
                   opts)))

(defn get-counter-column-range
  "Queries for a range of counter columns."
  [ks cf pk start end & o]
  (let [opts (extract-options o cf)
        ns (s/serializer (:n-serializer opts))]
    (execute-query (doto (HFactory/createCounterSliceQuery ks type-inferring ns)
                     (.setColumnFamily cf)
                     (.setKey pk)
                     (.setRange start end (:reversed opts) (:limit opts)))
                   opts)))

(defn get-counter-super-columns
  "Queries for counter values in a super column column family."
  [ks cf pk sc c & opts]
  (let [o (extract-options opts cf)
        ss (s/serializer (:s-serializer o))
        ns (s/serializer (:n-serializer o))]
    (execute-query (doto (HFactory/createSuperSliceCounterQuery ks type-inferring ss ns)
                     (.setKey pk)
                     (.setColumnFamily cf)
                     (.setColumnNames (into-array c))
                     (.setRange (:start o) (:end o) (:reversed o) (:limit o)))
                   opts)))

(defn delete-columns
  "Deletes columns identified in cs for row pk."
  [ks cf pk cs & o]
  (let [mut (HFactory/createMutator ks type-inferring)
        opts (apply hash-map o)
        serializer (s/serializer (or (:n-serializer opts) type-inferring))]
    (doseq [c cs] (.addDeletion mut pk cf c serializer))
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
                                               opts))))
    (.execute mut)))

(defn delete-rows
  "Deletes all columns for rows identified in pks sequence."
  [ks cf pks]
  (let [mut (HFactory/createMutator ks type-inferring)]
    (doseq [k pks] (.addDeletion mut k cf))
    (.execute mut)))

(defn count-columns
  "Counts number of columns for pk in column family cf. The method is not O(1).
   It takes all the columns from disk to calculate the answer. The only benefit
   of the method is that you do not need to pull all the columns over Thrift
   interface to count them."
  [ks pk cf & {:keys [start end limit] :or {start nil
                                            end nil
                                            limit Integer/MAX_VALUE}}]
  (execute-query (doto (HFactory/createCountQuery ks
                                                  type-inferring
                                                  (s/serializer :bytes))
                   (.setKey pk)
                   (.setRange start end limit)
                   (.setColumnFamily cf))))

(defn schemas-by-name
  [schemas]
  (->> schemas
       (map (fn [{:keys [name] :as opts}]
              [name opts]))
       (into {})))

(defmacro with-schemas
  "Binds schema information to *schemas*. Allows other get-xxx functions
   to re-use column family information and provide sensible default serializers
   without having to specify every time."
  [schemas & body]
  `(binding [*schemas* (schemas-by-name ~schemas)]
     ~@body))
