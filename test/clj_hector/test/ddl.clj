(ns clj-hector.test.ddl
  (:use [clj-hector.core]
        [clj-hector.test.cassandra-helper :only (with-test-cluster)])
  (:use [clj-hector.ddl] :reload)
  (:use [clojure.test]))

(deftest should-add-remove-a-keyspace
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"}
                                       {:name "b"
                                        :comparator :long}]})
      (let [kss (map :name (keyspaces cluster))]
        (is (= 1 (count (filter (partial = random-ks) kss)))))
      (drop-keyspace cluster random-ks)
      (let [kss (map :name (keyspaces cluster))]
        (is (= 0 (count (filter (partial = random-ks) kss))))))))

(deftest should-add-remove-column-families
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"}
                                       {:name "b"
                                        :comparator :long}]})
      (add-column-family cluster random-ks {:name "c" :comparator :long})
      (let [column-families (column-families cluster random-ks)
            column-family (first (filter (fn [cf] (= (:name cf) "c")) column-families))]
        (is (not (nil? column-family)))
        (is (= :long (:comparator column-family))))
      (drop-column-family cluster random-ks "a")
      (drop-column-family cluster random-ks "b")
      (drop-column-family cluster random-ks "c")    
      (let [column-families (column-families cluster random-ks)
            column-family (filter (fn [cf] (= (:name cf) "c")) column-families)]
        (is (empty? column-families)))
      (drop-keyspace cluster random-ks)
      (let [kss (map :name (keyspaces cluster))]
        (is (= 0 (count (filter (partial = random-ks) kss))))))))

(deftest should-add-remove-counter-column-family
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"
                                        :validator :long}]})
      (add-column-family cluster random-ks {:name "b" :comparator :long :type :super})
      (is (= {:name "b"
              :comparator :long
              :type :super
              :validator :bytes
              :k-validator :bytes}
             (dissoc (first (filter #(= (:name %) "b")
                                    (column-families cluster random-ks)))
                     :id
                     :column-metadata)))
      (drop-keyspace cluster random-ks))))

(deftest should-add-remove-super-column-family
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"
                                        :comparator :long}]})
      (add-column-family cluster random-ks {:name "b" :comparator :long :type :super})
      (is (= {:name "b"
              :comparator :long
              :type :super
              :validator :bytes
              :k-validator :bytes}
             (dissoc (first (filter #(= (:name %) "b")
                                    (column-families cluster random-ks)))
                     :id
                     :column-metadata)))
      (drop-keyspace cluster random-ks))))

(deftest should-create-counter-column
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"
                                        :validator :counter}]})
      (is (= {:name "a"
              :comparator :bytes
              :type :standard
              :validator :counter
              :k-validator :bytes}
             (dissoc (first (column-families cluster random-ks)) 
                     :id
                     :column-metadata)))
      (drop-keyspace cluster random-ks))))


(deftest should-add-remove-column-families-with-column-meta-data
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"
                                        :type :standard
                                        :column-metadata
                                        [{:name "col"
                                          :index-name "colidx"
                                          :index-type :keys
                                          :validator :utf-8}
                                         {:name "coltwo"
                                          :validator :integer}]}]})
      (is (= {:name "a"
              :comparator :bytes
              :type :standard
              :validator :bytes
              :k-validator :bytes}
             (dissoc (first (column-families cluster random-ks))
                     :id
                     :column-metadata)))
      (drop-keyspace cluster random-ks))))

(deftest should-add-update-remove-column-families-with-column-meta-data
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"
                                        :type :standard
                                        :column-metadata
                                        [{:name "col"
                                          :index-name "colidx"
                                          :index-type :keys
                                          :validator :utf-8}
                                         ]}]})
      (is (= {:name "a"
              :comparator :bytes
              :type :standard
              :validator :bytes
              :k-validator :bytes}
             (dissoc (first (column-families cluster random-ks))
                     :id
                     :column-metadata)))
      (let [orig-cf (first (column-families cluster random-ks))
            new-cf (assoc-in orig-cf [:column-metadata]
                                        [{:name "col"
                                          :index-name "colidx"
                                          :index-type :keys
                                          :validator :utf-8}
                                        {:name "coltwo" :validator :integer}
                                        {:name "colthree" :validator :boolean}] )]

        (update-column-family cluster random-ks new-cf))
        ;; Column names need to be converted to strings to test for equality.
        (let [actual (map ;convert col names to string
                       #(assoc % :name (String. (:name %)))
                       (:column-metadata (first (column-families cluster random-ks))))
              expected [{:name "col"
                         :index-name "colidx",
                         :index-type :keys,
                         :validator :utf-8}
                        {:name "coltwo"
                         :validator :integer}
                        {:name "colthree" :validator :boolean}]]
          (is (= (set expected) (set actual))))

(drop-keyspace cluster random-ks))))

(deftest should-add-remove-column-families-with-k-validator
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"
                                        :type :standard
                                        :k-validator :uuid
                                        :column-metadata
                                        [{:name "col"
                                          :index-name "colidx"
                                          :index-type :keys
                                          :validator :utf-8}
                                         {:name "coltwo"
                                          :validator :integer}]}]})
      (is (= {:name "a"
              :comparator :bytes
              :type :standard
              :validator :bytes
              :k-validator :uuid}
             (dissoc (first (column-families cluster random-ks)) 
                     :id
                     :column-metadata)))
      (drop-keyspace cluster random-ks))))

(deftest should-return-dynamic-composite-comparator
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (with-test-cluster cluster
      (add-keyspace cluster
                    {:name random-ks
                     :strategy :simple
                     :replication 1
                     :column-families [{:name "a"
                                        :type :standard
                                        :k-validator :uuid
                                        :comparator :dynamic-composite
                                        :comparator-alias
                                        "(t=>TimeUUIDType,s=>UTF8Type)"}]})
      (is (= {:comparator :dynamic-composite
              :comparator-alias
              "(t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type)"}
             (select-keys (first (column-families cluster random-ks))
                          [:comparator :comparator-alias])))
      (drop-keyspace cluster random-ks))))
