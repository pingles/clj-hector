(ns clj-hector.test.core
  (:use [clj-hector.core] :reload-all)
  (:use [clojure.test])
  (:require [clj-hector.ddl :as ddl]
            [clj-hector.serialize :as ser])
  (:import [me.prettyprint.cassandra.serializers StringSerializer IntegerSerializer LongSerializer]
           [me.prettyprint.hector.api Serializer]))

(def *test-cluster* (cluster "test" "localhost"))

(deftest string-key-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:v-serializer :string
              :n-serializer :string]]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put ks cf "row-key" {"k" "v"})
    (is (= '({"row-key" {"k" "v"}})
           (apply get-rows ks cf ["row-key"] opts)))
    (is (= {"k" "v"}
           (apply get-columns ks cf "row-key" ["k"] opts)))
    (delete-columns ks cf "row-key" ["k"])
    (is (= '({"row-key" {}})
           (apply get-rows ks cf ["row-key"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest custom-serializer
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:v-serializer (StringSerializer/get)
              :n-serializer :string]]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put ks cf "row-key" {"k" "v"})
    (is (= {"k" "v"}
           (apply get-columns ks cf "row-key" ["k"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest string-name-int-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:v-serializer :integer
              :n-serializer :string]]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put ks cf "row-key" {"k" 1234})
    (is (= '({"row-key" {"k" 1234}})
           (apply get-rows ks cf ["row-key"] opts)))
    (is (= {"k" 1234}
           (apply get-columns ks cf "row-key" ["k"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest string-key-long-name-and-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:n-serializer :long
              :v-serializer :long]]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :comparator :long}]})
    (put ks cf "row-key" {(long 1) (long 1234)})
    (is (= {"row-key" {(long 1) (long 1234)}}
           (first (apply get-rows ks cf ["row-key"] opts))))
    (is (= {(long 1) (long 1234)}
           (apply get-columns ks cf "row-key" [(long 1)] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest long-key-long-name-and-values
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:n-serializer :long
              :v-serializer :long]]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :comparator :long}]})
    (put ks cf (long 101) {(long 1) (long 1234)})
    (is (= {(long 101) {(long 1)(long 1234)}}
           (first (apply get-rows ks cf [(long 101)] opts))))
    (is (= {(long 1) (long 1234)}
           (apply get-columns ks cf (long 101) [(long 1)] opts)))
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
    (put ks cf "row-key" {(long 1) (long 101)
                          (long 2) (long 102)
                          (long 3) (long 103)
                          (long 4) (long 104)})
    (is (= {"row-key" (sorted-map (long 2) (long 102)
                                  (long 3) (long 103))}
           (first (apply get-rows ks cf ["row-key"] [:n-serializer :long
                                                     :v-serializer :long
                                                     :start (long 2)
                                                     :end (long 3)]))))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest defaults-to-byte-array-for-name-value-serialization
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put ks cf "row-key" {"k" "v"})
    (let [first-row (get (first (get-rows ks cf ["row-key"])) "row-key")
          n-bytes (first (keys first-row))
          v-bytes (first (vals first-row))]
      (is (= "k"
             (String. n-bytes)))
      (is (= "v"
             (String. v-bytes))))
    (let [res (get-columns ks cf "row-key" [(.getBytes "k")])
          n-bytes (first (keys res))
          v-bytes (first (vals res))]
      (is (= "k"
             (String. n-bytes)))
      (is (= "v"
             (String. v-bytes))))
    (let [res (apply get-columns ks cf "row-key" ["k"] [:n-serializer :string])
          n (first (keys res))
          v-bytes (first (vals res))]
      (is (= "k" n))
      (is (= "v"
             (String. v-bytes))))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest counting
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf}]})
    (put ks cf "row-key" {"k" "v" "k2" "v2"})
    (is (= {:count 2}
           (count-columns ks "row-key" cf)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest supercolumn-with-string-key-name-and-value
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:v-serializer :string
              :n-serializer :string
              :s-serializer :string]]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :type :super}]})
    (put ks cf "row-key" {"SuperCol" {"k" "v"
                                      "k2" "v2"}
                          "SuperCol2" {"k" "v"
                                       "k2" "v2"}})
    (is (= :super
           (:type (first (ddl/column-families *test-cluster* ks-name)))))
    (is (= {"row-key" [{"SuperCol" {"k" "v"
                                    "k2" "v2"}}
                       {"SuperCol2" {"k" "v"
                                     "k2" "v2"}}]} 
           (first (apply get-super-rows ks cf ["row-key"] ["SuperCol" "SuperCol2"] opts))))
    (is (= {"k2" "v2"}
           (apply get-super-columns ks cf "row-key" "SuperCol" ["k2" "v2"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest deleting-supercolumns
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:v-serializer :string
              :n-serializer :string
              :s-serializer :string]]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :type :super}]})
    (put ks cf "row-key" {"SuperCol" {"k" "v"
                                      "k2" "v2"}
                          "SuperCol2" {"k" "v"
                                       "k2" "v2"}})
    (is (= {"k2" "v2"
            "k" "v"}
           (apply get-super-columns ks cf "row-key" "SuperCol" ["k" "k2"] opts)))
    (apply delete-super-columns ks cf {"row-key" {"SuperCol" ["k2"] "SuperCol2" ["k2"]}} opts)
    (is (= {"k" "v"}
           (apply get-super-columns ks cf "row-key" "SuperCol" ["k" "k2"] opts)))
    (is (= {"k" "v"}
           (apply get-super-columns ks cf "row-key" "SuperCol2" ["k" "k2"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest counter-columns
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:v-serializer :long
              :n-serializer :string]
        pk "row-key"]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :validator :counter}]})
    (put-counter ks cf pk {"name" 1 "other" 2})
    (is (= {"name" 1}
           (apply get-counter-columns ks cf pk ["name"] opts)))
    (is (= {"name" 1
            "other" 2}
           (apply get-counter-columns ks cf pk ["name" "other"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))

(deftest counter-super-columns
  (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
        cf "a"
        ks (keyspace *test-cluster* ks-name)
        opts [:v-serializer :long
              :n-serializer :string
              :s-serializer :string]
        pk "row-key"]
    (ddl/add-keyspace *test-cluster* {:name ks-name
                                      :strategy :local
                                      :replication 1
                                      :column-families [{:name cf
                                                         :type :super
                                                         :validator :counter}]})
    (put-counter ks cf pk {"SuperCol" {"name" 1
                                       "other" 2}})
    (is (= {"SuperCol" {"name" 1
                        "other" 2}}
           (apply get-counter-super-columns ks cf "row-key" "SuperCol" ["name" "other"] opts)))
    (ddl/drop-keyspace *test-cluster* ks-name)))
