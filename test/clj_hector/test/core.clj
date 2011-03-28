(ns clj-hector.test.core
  (:use [clj-hector.core] :reload)
  (:use [clojure.test])
  (:require [clj-hector.ddl :as ddl])
  (:import [me.prettyprint.cassandra.serializers StringSerializer]))

(def *test-cluster* (cluster "Pauls Cluster" "localhost"))

(deftest serializer-lookup
  (is (instance? StringSerializer
                 (serializer "Hello"))))

(deftest storing-and-reading-strings
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        random (str (java.util.UUID/randomUUID))
        cf "a"
        ks (keyspace *test-cluster* ks-name)]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put-row ks cf "row-key" {"k" "v"})
    (testing "rows"
      (is (= '({:key "row-key"
                :columns {"k" "v"}})
             (get-rows ks cf ["row-key"]))))
    (ddl/drop-keyspace *test-cluster* ks-name)))

