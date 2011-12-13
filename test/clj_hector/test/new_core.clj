(ns clj-hector.test.new-core
  (:use [clojure.test]
        [clj-hector.test.cassandra-helper :only (with-test-keyspace)]
        [clj-hector.core :only (put get-columns get-rows)] :reload))

(deftest serializers
  (with-test-keyspace keyspace [{:name "A"}]
    (let [column-family "A"
          opts [:v-serializer :string
                :n-serializer :string]]
      (put keyspace column-family "row-key" {"n" "v"})
      (is (= [{"row-key" {"n" "v"}}]
             (apply get-rows keyspace column-family ["row-key"] opts))
          (= {"n" "v"}
             (apply get-columns keyspace column-family "row-key" ["n"] opts))))))
