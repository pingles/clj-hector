(ns clj-hector.test.new-core
  (:import [me.prettyprint.cassandra.serializers StringSerializer])
  (:use [clojure.test]
        [clj-hector.test.cassandra-helper :only (with-test-keyspace)]
        [clj-hector.core :only (put get-columns get-column-range get-rows)] :reload))

(deftest serializers
  (let [column-family "A"]
    (with-test-keyspace keyspace [{:name column-family}]
      (testing ":string serializer"
        (let [opts [:v-serializer :string
                    :n-serializer :string]]
          (put keyspace column-family "row-key" {"n" "v"})
          (is (= [{"row-key" {"n" "v"}}]
                 (apply get-rows keyspace column-family ["row-key"] opts))
              (= {"n" "v"}
                 (apply get-columns keyspace column-family "row-key" ["n"] opts)))))
      (testing "integers"
        (let [opts [:v-serializer :integer
                    :n-serializer :string]]
          (put keyspace column-family "row-key" {"n" (Integer/valueOf 1234)})
          (is (= {"n" 1234}
                 (apply get-columns keyspace column-family "row-key" ["n"] opts)))))
      (testing "custom serializer"
        (let [opts [:v-serializer (StringSerializer/get)
                    :n-serializer :string]]
          (put keyspace column-family "row-key" {"n" "v"})
          (is (= {"n" "v"}
                 (apply get-columns keyspace column-family "row-key" ["n"] opts)))))
      (testing "names and values serialize as bytes by default"
        (put keyspace column-family "row-key" {"n" "v"})
        (let [first-row (-> (get-rows keyspace column-family ["row-key"])
                            (first)
                            (get "row-key"))]
          (is (= "n"
                 (String. (first (keys first-row))))
              (= "v"
                 (String. (first (vals first-row))))))))))

(deftest column-comparator
  (with-test-keyspace keyspace [{:name "A" :comparator :long}]
    (let [column-family "A"
          opts [:n-serializer :long
                :v-serializer :long]]
      (put keyspace column-family "row-key" {(long 1) (long 1)
                                             (long 3) (long 3)
                                             (long 2) (long 2)})
      (is (= {"row-key" (sorted-map (long 1) (long 1)
                                    (long 2) (long 2)
                                    (long 3) (long 3))}
             (-> (apply get-rows keyspace column-family ["row-key"] opts)
                 (first)))))))

(deftest column-ranges
  (with-test-keyspace keyspace [{:name "A"}]
    (let [column-family "A"
          opts [:v-serializer :string
                :n-serializer :string]]
      (put keyspace column-family "row-key" {"a" "v"
                                             "b" "v"
                                             "c" "v"
                                             "d" "v"})
      (is (= {"a" "v"
              "b" "v"
              "c" "v"}
             (apply get-column-range keyspace column-family "row-key" "a" "c" opts))))))
