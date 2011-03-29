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
        ks (keyspace *test-cluster* ks-name)]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put-row ks cf "row-key" {"k" "v"})
    (is (= '({:key "row-key"
              :columns {"k" "v"}})
           (get-rows ks cf ["row-key"])))
    (is (= {"k" "v"}
           (get-columns ks cf "row-key" ["k"])))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest string-key-int-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put-row ks cf "row-key" {"k" 1234})
    (is (= '({:key "row-key"
              :columns {"k" 1234}})
           (get-rows ks cf ["row-key"] {:v-serializer :integer})))
    (is (= {"k" 1234}
           (get-columns ks cf "row-key" ["k"] {:v-serializer :integer})))
    (ddl/drop-keyspace *test-cluster* ks-name)))
