(ns clj-hector.test.schema
  (:require [clj-hector.ddl :as ddl])
  (:use [clj-hector.core]
        [clojure.test]))

(def test-cluster (cluster "test" "localhost"))
(def column-family "MyColumnFamily")

(defschema MyColumnFamily [:n-serializer :string
                           :v-serializer :string])

(deftest string-key-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        ks (keyspace test-cluster ks-name)]
    (ddl/add-keyspace test-cluster {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name column-family}]})
    (with-schemas [MyColumnFamily]
      (put ks column-family "row-key" {"k" "v"})
      (is (= '({"row-key" {"k" "v"}})
             (get-rows ks column-family ["row-key"])))
      (is (= {"k" "v"}
             (get-columns ks column-family "row-key" ["k"])))
      (delete-columns ks column-family "row-key" ["k"])
      (is (= '({"row-key" {}})
             (get-rows ks column-family ["row-key"]))))
    (ddl/drop-keyspace test-cluster ks-name)))
