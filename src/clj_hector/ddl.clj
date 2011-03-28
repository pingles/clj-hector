(ns clj-hector.ddl
  (:import [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.hector.api.ddl ComparatorType ColumnFamilyDefinition]))

(defn- make-column-family
  "Returns an object defining a new column family"
  ([keyspace column-family-name]
     (HFactory/createColumnFamilyDefinition keyspace column-family-name))
  ([keyspace column-family-name comparator-type]
     (let [cmp (condp = comparator-type
                   :ascii         ComparatorType/ASCIITYPE
                   :byte          ComparatorType/BYTESTYPE
                   :integer       ComparatorType/INTEGERTYPE
                   :lexical-uuid  ComparatorType/LEXICALUUIDTYPE
                   :long          ComparatorType/LONGTYPE
                   :time-uuid     ComparatorType/TIMEUUIDTYPE
                   :utf-8         ComparatorType/UTF8TYPE
                   (throw (Exception. "Unknown comparator type passed in column family definition")))]
       (HFactory/createColumnFamilyDefinition keyspace column-family-name cmp))))

(defn- make-keyspace-definition
  ([keyspace strategy-class replication-factor column-families]
     (let [column-families (map (fn [{:keys [name comparator]}]
                                  (if (nil? comparator)
                                    (make-column-family keyspace name)
                                    (make-column-family keyspace name comparator)))
                                column-families)]
       (HFactory/createKeyspaceDefinition keyspace
                                          strategy-class
                                          replication-factor
                                          column-families))))

(defn add-column-family
  "Adds a column family to a keyspace"
  ([cluster keyspace {:keys [name comparator]}]
     (if (nil? comparator)
       (.addColumnFamily cluster (make-column-family keyspace name))
       (.addColumnFamily cluster (make-column-family keyspace name comparator)))))

(defn drop-column-family
  "Removes a column family from a keyspace"
  ([cluster keyspace-name column-family-name]
     (.dropColumnFamily cluster keyspace-name column-family-name)))

(defn add-keyspace
  "Creates a new keyspace from the definition passed as a map"
  ([cluster {:keys [name strategy replication column-families]}]
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
  ([cluster keyspace-name]
     (.dropKeyspace cluster keyspace-name)))

(defn keyspaces
  "Description of the keyspaces available in the Cassandra cluster"
  ([cluster]
     (let [kss (.describeKeyspaces cluster)]
       (map (fn [ks] {:name (.getName ks)
                     :replication-factor (.getReplicationFactor ks)})
            kss))))


(defn- parse-comparator
  ([comparator-type]
     (condp = (.getClassName comparator-type)
       "org.apache.cassandra.db.marshal.UTF8Type"        :utf-8
       "org.apache.cassandra.db.marshal.AsciiType"       :ascii
       "org.apache.cassandra.db.marshal.BytesType"       :byte
       "org.apache.cassandra.db.marshal.IntegerType"     :integer
       "org.apache.cassandra.db.marshal.LexicalUUIDType" :lexical-uuid
       "org.apache.cassandra.db.marshal.LongType"        :long
       "org.apache.cassandra.db.marshal.TimeUUIDType"    :time-uuid)))

(defn column-families
  "Returns all the column families for a certain keyspace"
  ([cluster keyspace]
     (let [ks (first (filter (fn [ks] (= (.getName ks) keyspace))
                             (.describeKeyspaces cluster)))
           cf-defs (.getCfDefs ks)]
       (map (fn [cf-def]
              {:name (.getName cf-def)
               :comparator (parse-comparator (.getComparatorType cf-def))})
            cf-defs))))
