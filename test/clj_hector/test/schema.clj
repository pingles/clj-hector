(ns clj-hector.test.schema
  (:require [clj-hector.ddl :as ddl])
  (:require [clj-hector.time :as time])
  (:use [clojure.test]
        [clj-hector.test.cassandra-helper :only (with-test-keyspace)]
        [clj-hector.core] :reload))

(def column-family "MyColumnFamily")
(def MyColumnFamily {:name column-family
                     :n-serializer :string
                     :v-serializer :string})

(def UuidKeyedColumnFamily {:name "uuidcf" :n-serializer :string :v-serializer :string :k-validator :uuid})
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

(deftest uuid-keys
  (with-test-keyspace ks [UuidKeyedColumnFamily]
    (with-schemas [UuidKeyedColumnFamily]
      (let [rk (time/uuid-now) ]
        (put ks (:name UuidKeyedColumnFamily) rk {"k" "v"})

        (is (= (into '() [{rk {"k" "v"}}])
               (get-rows ks (:name UuidKeyedColumnFamily) [rk])))

        (is (thrown? me.prettyprint.hector.api.exceptions.HInvalidRequestException
                     (put ks (:name UuidKeyedColumnFamily) "row-key" {"k" "v"})))))))


