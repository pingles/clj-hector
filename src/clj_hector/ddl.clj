(ns ^{:author "Antonio Garrote, Paul Ingles, Ryan Fowler"
      :description "Functions to define Cassandra schemas: create/delete keyspaces, column families etc."}
  clj-hector.ddl
  (:import [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.cassandra.service ThriftCfDef]
           [me.prettyprint.hector.api.beans Composite AbstractComposite$ComponentEquality]
           [me.prettyprint.hector.api.ddl ComparatorType ColumnFamilyDefinition ColumnType KeyspaceDefinition ColumnIndexType]
           [me.prettyprint.cassandra.model BasicColumnDefinition BasicColumnFamilyDefinition]
           [me.prettyprint.cassandra.serializers StringSerializer BytesArraySerializer]))


(def component-equality-type {:less_than_equal    AbstractComposite$ComponentEquality/LESS_THAN_EQUAL
                              :equal              AbstractComposite$ComponentEquality/EQUAL
                              :greater_than_equal AbstractComposite$ComponentEquality/GREATER_THAN_EQUAL})

(def comparator-types {:ascii             ComparatorType/ASCIITYPE
                       :bytes             ComparatorType/BYTESTYPE
                       :integer           ComparatorType/INTEGERTYPE
                       :lexical-uuid      ComparatorType/LEXICALUUIDTYPE
                       :local-partitioner ComparatorType/LOCALBYPARTITIONERTYPE
                       :long              ComparatorType/LONGTYPE
                       :time-uuid         ComparatorType/TIMEUUIDTYPE
                       :utf-8             ComparatorType/UTF8TYPE
                       :composite         ComparatorType/COMPOSITETYPE
                       :dynamic-composite ComparatorType/DYNAMICCOMPOSITETYPE
                       :uuid              ComparatorType/UUIDTYPE
                       :counter           ComparatorType/COUNTERTYPE})

(def validator-types {:ascii             "AsciiType"
                      :bytes             "BytesType"
                      :integer           "IntegerType"
                      :lexical-uuid      "LeixcalUUIDType"
                      :local-partitioner "LocalByPartionerType"
                      :long              "LongType"
                      :time-uuid         "TimeUUIDType"
                      :utf-8             "UTF8Type"
                      :composite         "CompositeType"
                      :dynamic-composite "DynamicCompositeType"
                      :uuid              "UUIDType"
                      :counter           "CounterColumnType"})

(def column-index-types {:keys ColumnIndexType/KEYS})

(defn- column-type
  [type]
  (if (= :super type)
    ColumnType/SUPER
    ColumnType/STANDARD))

(defn- default-validation-class
  [validator]
  (get validator-types (or validator :bytes)))

(defn- make-column
  "Returns an object defining a column with validation and optional index"
  ([{:keys [name index-name index-type validator id]}]
   (let [c-def (BasicColumnDefinition.)
         name (.toByteBuffer (StringSerializer/get) name)]
     (doto c-def
       (.setName name)
       (.setValidationClass (.getClassName (comparator-types validator))))
     (if index-type
       (.setIndexType c-def (column-index-types index-type)))
     (if index-name
       (.setIndexName c-def index-name))
     c-def)))

(defn- make-column-family
  "Returns an object defining a new column family"
  ([keyspace {:keys [name type comparator comparator-alias validator k-validator column-metadata id replicate-on-write]}]
     (let [cf-def (BasicColumnFamilyDefinition.)
           columns (map make-column column-metadata)]
       (doto ^BasicColumnFamilyDefinition cf-def
         (.setName name)
         (.setKeyspaceName keyspace)
         (.setColumnType (column-type type))
         (.setDefaultValidationClass (default-validation-class validator)))
       (when id
         (.setId cf-def id))
       (if k-validator
         (.setKeyValidationClass cf-def (validator-types k-validator)))
       (if comparator
         (.setComparatorType cf-def (comparator-types comparator)))
       (if comparator-alias
         (.setComparatorTypeAlias cf-def comparator-alias))
       (if replicate-on-write
         (.setReplicateOnWrite cf-def replicate-on-write))
       (doseq [column columns] (.addColumnDefinition cf-def column))
       (ThriftCfDef. cf-def))))

(defn- make-keyspace-definition
  ([keyspace strategy-class replication-factor column-families]
     (let [column-families (map (fn [column-family]
                                  (make-column-family keyspace column-family))
                                column-families)]
       (HFactory/createKeyspaceDefinition keyspace
                                          strategy-class
                                          replication-factor
                                          column-families))))

(defn add-column-family
  "Adds a column family to a keyspace"
  ([^Cluster cluster keyspace opts]
       (.addColumnFamily cluster (make-column-family keyspace opts))))

(defn update-column-family
  "Updates a column family in a keyspace"
  ([^Cluster cluster keyspace opts]
       (.updateColumnFamily cluster (make-column-family keyspace opts))))

(defn drop-column-family
  "Removes a column family from a keyspace"
  ([^Cluster cluster keyspace-name column-family-name]
     (.dropColumnFamily cluster keyspace-name column-family-name)))

(defn add-keyspace
  "Creates a new keyspace from the definition passed as a map"
  ([^Cluster cluster {:keys [name strategy replication column-families]}]
     (let [strategy (condp = strategy
                        :local            "org.apache.cassandra.locator.LocalStrategy"
                        :network-topology "org.apache.cassandra.locator.NetworkTopologyStrategy"
                        "org.apache.cassandra.locator.SimpleStrategy")
           replication (or replication 1)]
       (.addKeyspace cluster (make-keyspace-definition name
                                                       strategy
                                                       replication
                                                       column-families)))))

(defn drop-keyspace
  "Deletes a whole keyspace from the cluster"
  ([^Cluster cluster keyspace-name]
     (.dropKeyspace cluster keyspace-name)))

(defn keyspaces
  "Description of the keyspaces available in the Cassandra cluster"
  ([^Cluster cluster]
     (let [kss (.describeKeyspaces cluster)]
       (map (fn [^KeyspaceDefinition ks] {:name (.getName ks)
                                         :replication-factor (.getReplicationFactor ks)})
            kss))))

(defn- parse-type
  [x]
  (if (= ColumnType/SUPER x)
    :super
    :standard))

(def types {"org.apache.cassandra.db.marshal.AsciiType"            :ascii
            "org.apache.cassandra.db.marshal.BytesType"            :bytes
            "org.apache.cassandra.db.marshal.IntegerType"          :integer
            "org.apache.cassandra.db.marshal.LexicalUUIDType"      :lexical-uuid
            "org.apache.cassandra.db.marshal.LocalByPartionerType" :local-partitioner
            "org.apache.cassandra.db.marshal.LongType"             :long
            "org.apache.cassandra.db.marshal.TimeUUIDType"         :time-uuid
            "org.apache.cassandra.db.marshal.UTF8Type"             :utf-8
            "org.apache.cassandra.db.marshal.CompositeType"        :composite
            "org.apache.cassandra.db.marshal.DynamicCompositeType" :dynamic-composite
            "org.apache.cassandra.db.marshal.UUIDType"             :uuid
            "org.apache.cassandra.db.marshal.CounterColumnType"    :counter})

(defn- parse-comparator
  [^ComparatorType comparator-type]
  (get types (.getClassName comparator-type)))

(defn- convert-metadata [cf-m]
  (let [base {:name (.fromByteBuffer (BytesArraySerializer/get) (.getName cf-m))
              :validation-class (.get types (.getValidationClass cf-m))}]
    (if (.getIndexName cf-m)
      (assoc base
        :index-type (keyword (clojure.string/lower-case (.name (.getIndexType cf-m))))
        :index-name (.getIndexName cf-m))
      base)))

(defn column-families
  "Returns all the column families for a certain keyspace"
  ([^Cluster cluster ^String keyspace]
     (let [ks (first (filter (fn [^KeyspaceDefinition ks] (= (.getName ks) keyspace))
                             (.describeKeyspaces cluster)))
           cf-defs (.getCfDefs ^KeyspaceDefinition ks)]
       (map (fn [^ColumnFamilyDefinition cf-def]
              {:id (.getId cf-def)
               :name (.getName cf-def)
               :comparator (parse-comparator (.getComparatorType cf-def))
               :type (parse-type (.getColumnType cf-def))
               :validator (get types (.getDefaultValidationClass cf-def))
               :k-validator (get types (.getKeyValidationClass cf-def))
               :column-metadata (map convert-metadata (.getColumnMetadata cf-def))})
            cf-defs))))
