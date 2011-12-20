(ns clj-hector.test.core
  (:import [me.prettyprint.cassandra.serializers StringSerializer])
  (:use [clojure.test]
        [clj-hector.test.cassandra-helper :only (with-test-keyspace)]
        [clj-hector.core] :reload))

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

;; count-columns is different to counter columns, it's not
;; an O(1) operation.
(deftest counting-with-count-columns
  (let [column-family "A"]
    (with-test-keyspace keyspace [{:name column-family}]
      (put keyspace column-family "row-key" {"n" "v" "n2" "v2"})
      (is (= {:count 2}
             (count-columns keyspace "row-key" column-family))))))

(deftest super-columns
  (let [column-family "A"
        opts [:s-serializer :string :n-serializer :string :v-serializer :string]]
    (with-test-keyspace keyspace [{:name column-family :type :super}]
      (put keyspace column-family "row-key" {"SuperCol" {"n" "v" "n2" "v2"}
                                             "SuperCol2" {"n" "v" "n2" "v2"}})
      (testing "querying"
        (is (= {"row-key" [{"SuperCol" {"n" "v"
                                        "n2" "v2"}}
                           {"SuperCol2" {"n" "v"
                                         "n2" "v2"}}]}
               (first (apply get-super-rows keyspace column-family ["row-key"] ["SuperCol" "SuperCol2"] opts))))
        (is (= {"n" "v" "n2" "v2"}
               (apply get-super-columns keyspace column-family "row-key" "SuperCol" ["n" "n2"] opts))))
      (testing "deletion"
        (put keyspace column-family "row-key" {"SuperCol" {"n" "v" "n2" "v2"}
                                               "SuperCol2" {"n" "v"}})
        (apply delete-super-columns keyspace column-family {"row-key" {"SuperCol" ["n2"]}} opts)
        (is (= {"n" "v"}
               (apply get-super-columns keyspace column-family "row-key" "SuperCol" ["n" "n2"] opts)))))))

(deftest counter-columns
  (testing "regular column families"
    (let [column-family "A"
          opts [:n-serializer :string :v-serializer :long]
          pk "row-key"]
      (with-test-keyspace keyspace [{:name column-family
                                     :validator :counter}]
        (put-counter keyspace column-family pk {"n" 1 "n2" 2})
        (is (= {"n" 1
                "n2" 2}
               (apply get-counter-columns keyspace column-family pk ["n" "n2"] opts))))))
  (testing "super column families"
    (let [column-family "A"
          opts [:s-serializer :string :n-serializer :string :v-serializer :long]
          pk "row-key"]
      (with-test-keyspace keyspace [{:name column-family
                                     :validator :counter
                                     :type :super}]
        (put-counter keyspace column-family pk {"SuperCol" {"n" 1 "n2" 2}})
        (is (= {"SuperCol" {"n" 1 "n2" 2}}
               (apply get-counter-super-columns keyspace column-family pk "SuperCol" ["n" "n2"] opts))))))
  (testing "counter column range query"
    (let [column-family "A"
          opts [:n-serializer :string :v-serializer :long]
          pk "row-key"]
      (with-test-keyspace keyspace [{:name column-family
                                     :validator :counter}]
        (put-counter keyspace column-family "row-key" {"a" 1 "b" 2 "c" 1 "d" 3})
        (is (= {"a" 1 "b" 2 "c" 1}
               (apply get-counter-column-range keyspace column-family pk "a" "c" opts)))))))

(deftest ttl-columns
  (testing "regular column ttl"
    (let [column-family "A"
          opts [:n-serializer :string :v-serializer :integer]
          pk "row-key1"]
      (with-test-keyspace keyspace [{:name column-family}]
        (put keyspace column-family pk {"n" 1 "n2" 2} 1)
        (is (= {"n" 1 "n2" 2}
               (apply get-columns keyspace column-family pk ["n" "n2"] opts)))
        (Thread/sleep 2000)
        (is (= {}
               (apply get-columns keyspace column-family pk ["n" "n2"] opts))))))
  (testing "super column ttl"
    (let [column-family "A"
          opts [:s-serializer :string :n-serializer :string :v-serializer :integer]
          pk "row-key"]
      (with-test-keyspace keyspace [{:name column-family
                                     :type :super}]
        (put keyspace column-family pk {"SuperCol" {"n" 1 "n2" 2}} 1)
        (is (= {"n" 1 "n2" 2}
               (apply get-super-columns keyspace column-family pk "SuperCol" ["n" "n2"] opts)))
        (Thread/sleep 2000)
        (is (= {}
               (apply get-super-columns keyspace column-family pk "SuperCol" ["n" "n2"] opts)))))))
