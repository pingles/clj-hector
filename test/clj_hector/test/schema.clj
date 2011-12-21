(ns clj-hector.test.schema
  (:require [clj-hector.ddl :as ddl])
  (:use [clojure.test]
        [clj-hector.test.cassandra-helper :only (with-test-keyspace)]
        [clj-hector.core] :reload))

(def column-family "MyColumnFamily")
(def MyColumnFamily {:name column-family
                     :n-serializer :string
                     :v-serializer :string})

(deftest string-key-values
  (with-test-keyspace ks [{:name column-family}]
    (with-schemas [MyColumnFamily]
      (put ks column-family "row-key" {"k" "v"})
      (is (= '({"row-key" {"k" "v"}})
             (get-rows ks column-family ["row-key"])))
      (is (= {"k" "v"}
             (get-columns ks column-family "row-key" ["k"])))
      (delete-columns ks column-family "row-key" ["k"])
      (is (= '({"row-key" {}})
             (get-rows ks column-family ["row-key"]))))))
