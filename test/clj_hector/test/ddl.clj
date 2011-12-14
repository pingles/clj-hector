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
              :validator :bytes}
             (first (filter #(= (:name %) "b")
                            (column-families cluster random-ks)))))
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
              :validator :bytes}
             (first (filter #(= (:name %) "b")
                            (column-families cluster random-ks)))))
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
              :validator :counter}
             (first (column-families cluster random-ks))))
      (drop-keyspace cluster random-ks))))
