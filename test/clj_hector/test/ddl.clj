(ns clj-hector.test.ddl
  (:use [clj-hector.core])
  (:use [clj-hector.ddl] :reload)
  (:use [clojure.test]))

(def test-cassandra-cluster (cluster "test" "localhost"))

(deftest should-add-remove-a-keyspace
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (add-keyspace test-cassandra-cluster
                  {:name random-ks
                   :strategy :local
                   :replication 1
                   :column-families [{:name "a"}
                                     {:name "b"
                                      :comparator :long}]})
    (let [kss (map :name (keyspaces test-cassandra-cluster))]
      (is (= 1 (count (filter (partial = random-ks) kss)))))
    (drop-keyspace test-cassandra-cluster random-ks)
    (let [kss (map :name (keyspaces test-cassandra-cluster))]
      (is (= 0 (count (filter (partial = random-ks) kss)))))))

(deftest should-add-remove-column-families
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (add-keyspace test-cassandra-cluster
                  {:name random-ks
                   :strategy :local
                   :replication 1
                   :column-families [{:name "a"}
                                     {:name "b"
                                      :comparator :long}]})
    (add-column-family test-cassandra-cluster random-ks {:name "c" :comparator :long})
    (let [column-families (column-families test-cassandra-cluster random-ks)
          column-family (first (filter (fn [cf] (= (:name cf) "c")) column-families))]
      (is (not (nil? column-family)))
      (is (= :long (:comparator column-family))))
    (drop-column-family test-cassandra-cluster random-ks "a")
    (drop-column-family test-cassandra-cluster random-ks "b")
    (drop-column-family test-cassandra-cluster random-ks "c")    
    (let [column-families (column-families test-cassandra-cluster random-ks)
          column-family (filter (fn [cf] (= (:name cf) "c")) column-families)]
      (is (empty? column-families)))
    (drop-keyspace test-cassandra-cluster random-ks)
    (let [kss (map :name (keyspaces test-cassandra-cluster))]
      (is (= 0 (count (filter (partial = random-ks) kss)))))))

(deftest should-add-remove-counter-column-family
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (add-keyspace test-cassandra-cluster
                  {:name random-ks
                   :strategy :local
                   :replication 1
                   :column-families [{:name "a"
                                      :validator :long}]})
    (add-column-family test-cassandra-cluster random-ks {:name "b" :comparator :long :type :super})
    (is (= {:name "b"
            :comparator :long
            :type :super
            :validator :bytes}
           (first (filter #(= (:name %) "b")
                          (column-families test-cassandra-cluster random-ks)))))
    (drop-keyspace test-cassandra-cluster random-ks)))

(deftest should-add-remove-super-column-family
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (add-keyspace test-cassandra-cluster
                  {:name random-ks
                   :strategy :local
                   :replication 1
                   :column-families [{:name "a"
                                      :comparator :long}]})
    (add-column-family test-cassandra-cluster random-ks {:name "b" :comparator :long :type :super})
    (is (= {:name "b"
            :comparator :long
            :type :super
            :validator :bytes}
           (first (filter #(= (:name %) "b")
                          (column-families test-cassandra-cluster random-ks)))))
    (drop-keyspace test-cassandra-cluster random-ks)))

(deftest should-create-counter-column
  (let [random-ks (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")]
    (add-keyspace test-cassandra-cluster
                  {:name random-ks
                   :strategy :local
                   :replication 1
                   :column-families [{:name "a"
                                      :validator :counter}]})
    (is (= {:name "a"
            :comparator :bytes
            :type :standard
            :validator :counter}
           (first (column-families test-cassandra-cluster random-ks))))
    (drop-keyspace test-cassandra-cluster random-ks)))
