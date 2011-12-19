(ns clj-hector.consistency
  (:refer-clojure :exclude [get])
  (:import [me.prettyprint.hector.api HConsistencyLevel ConsistencyLevelPolicy]
           [me.prettyprint.cassandra.service OperationType]))

(def consistencies {:one HConsistencyLevel/ONE
                    :two HConsistencyLevel/TWO
                    :three HConsistencyLevel/THREE
                    :quorum HConsistencyLevel/QUORUM
                    :all HConsistencyLevel/ALL
                    :any HConsistencyLevel/ANY
                    :each-quorum HConsistencyLevel/EACH_QUORUM
                    :local-quorum HConsistencyLevel/LOCAL_QUORUM})

(def operation-types {OperationType/READ :read
                      OperationType/WRITE :write
                      OperationType/META_READ :meta-read
                      OperationType/META_WRITE :meta-write})

(defn policy [policy]
  (reify ConsistencyLevelPolicy
    (get [this operation-type cf]
        (let [cf-map (policy cf (policy "*"))
              type (operation-types operation-type)
              level (cf-map type :one)
              hlevel (consistencies level)]
          hlevel))
    (get [this operation-type] (.get this operation-type "*"))))
