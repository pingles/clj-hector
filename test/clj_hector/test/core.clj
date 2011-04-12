(ns clj-hector.test.core
  (:use [clj-hector.core] :reload)
  (:use [clojure.test])
  (:require [clj-hector.ddl :as ddl])
  (:import [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer]))

(def *test-cluster* (cluster "test" "localhost"))

(deftest serializer-lookup
  (is (instance? StringSerializer
                 (serializer "Hello")))
  (is (instance? IntegerSerializer
                 (serializer 1234)))
  (is (instance? LongSerializer
                 (serializer (long 1234)))))

(deftest string-key-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts {:v-serializer :string
              :n-serializer :string}]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put-row ks cf "row-key" {"k" "v"})
    (is (= '({:key "row-key"
              :columns {"k" "v"}})
           (get-rows ks cf ["row-key"] opts)))
    (is (= {"k" "v"}
           (get-columns ks cf "row-key" ["k"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest string-name-int-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts {:v-serializer :integer
              :n-serializer :string}]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put-row ks cf "row-key" {"k" 1234})
    (is (= '({:key "row-key"
              :columns {"k" 1234}})
           (get-rows ks cf ["row-key"] opts)))
    (is (= {"k" 1234}
           (get-columns ks cf "row-key" ["k"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest string-key-long-name-and-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts {:n-serializer :long
              :v-serializer :long}]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :comparator :long}]})
    (put-row ks cf "row-key" {(long 1) (long 1234)})
    (is (= {:key "row-key"
            :columns {(long 1) (long 1234)}}
           (first (get-rows ks cf ["row-key"] opts))))
    (is (= {(long 1) (long 1234)}
           (get-columns ks cf "row-key" [(long 1)] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest long-key-long-name-and-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts {:n-serializer :long
              :v-serializer :long}]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :comparator :long}]})
    (put-row ks cf (long 101) {(long 1) (long 1234)})
    (is (= {:key (long 101)
            :columns {(long 1) (long 1234)}}
           (first (get-rows ks cf [(long 101)] opts))))
    (is (= {(long 1) (long 1234)}
           (get-columns ks cf (long 101) [(long 1)] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))


(deftest string-key-long-name-and-values-with-range
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :comparator :long}]})
    (put-row ks cf "row-key" {(long 1) (long 101)
                              (long 2) (long 102)
                              (long 3) (long 103)
                              (long 4) (long 104)})
    (is (= {:key "row-key"
            :columns (sorted-map (long 2) (long 102)
                                 (long 3) (long 103))}
           (first (get-rows ks cf ["row-key"] {:n-serializer :long
                                               :v-serializer :long
                                               :start (long 2)
                                               :end (long 3)}))))
    (ddl/drop-keyspace *test-cluster* ks-name)))


(deftest counting
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put-row ks cf "row-key" {"k" "v" "k2" "v2"})
    (is (= {:count 2}
           (count-columns ks "row-key" cf)))
    (ddl/drop-keyspace *test-cluster* ks-name)))
