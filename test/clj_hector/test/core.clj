(ns clj-hector.test.core
  (:import [me.prettyprint.cassandra.serializers StringSerializer])
  (:require [clj-hector.serialize :as s]
            [clj-hector.time :as t])
  (:use [clojure.test]
        [clj-hector.test.test-helper]
        [clj-hector.test.cassandra-helper :only (with-test-keyspace)]
        [clj-hector.core] :reload))

(deftest composite-serializer
  (let [column-family "A"]
    (with-test-keyspace keyspace [{:name column-family
                                   :comparator :composite
                                   :comparator-alias "(UTF8Type, UTF8Type)"}]
      (testing ":composite serializer"
        (let [opts [:v-serializer :string
                    :n-serializer :composite
                    :c-serializer [:string :string]]
              comp (create-composite {:value "col"
                                      :n-serializer :string
                                      :comparator :utf-8}
                                     {:value "name"
                                      :n-serializer :string
                                      :comparator :utf-8})]

          (put keyspace column-family "row-key" {comp "v"} :n-serializer :composite)
          (is (= [{"row-key" {["col" "name"] "v"}}]
                 (apply get-rows keyspace column-family ["row-key"] opts))
              (= {["col" "name"] "v"}
                 (apply get-columns keyspace column-family "row-key" [comp] opts)))
          (is (instance? me.prettyprint.hector.api.mutation.MutationResult
                         (delete-columns keyspace column-family "row-key" [comp] :n-serializer :composite))))))))

(deftest serializers
  (let [column-family "A"]
    (with-test-keyspace keyspace [{:name column-family}]
      (testing ":dynamic-composite serializer"
        (let [opts [:v-serializer :string
                    :n-serializer :dynamic-composite]
              comp (create-dynamic-composite {:value "col"
                                              :n-serializer :ascii
                                              :comparator :ascii}
                                             {:value "name"
                                              :n-serializer :ascii
                                              :comparator :ascii})]

          (put keyspace column-family "row-key" {comp "v"} :n-serializer :dynamic-composite)
          (is (= [{"row-key" {["col" "name"] "v"}}]
                 (apply get-rows keyspace column-family ["row-key"] opts))
              (= {["col" "name"] "v"}
                 (apply get-columns keyspace column-family "row-key" [comp] opts))))))

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
                 (first))))))
  (testing "avoids array-map conversion to hash-map"
    (with-test-keyspace keyspace [{:name "A" :comparator :utf-8}]
      (let [column-family "A"
            num-strs (map str (range 10 50))
            opts [:n-serializer :string
                  :v-serializer :string]]
        (put keyspace column-family "row-key" (zipmap num-strs num-strs))
        (is (= num-strs
               (-> (apply get-rows keyspace column-family ["row-key"] opts)
                   first
                   vals
                   first
                   keys)))))))


;; The pair of UUIDs below were chosen because sorted-map and
;; Cassandra's TimeUUID comparator sort them differently. The Time 
;; portion of the UUID is important part for sorting, but the main
;; point is that clj-hector should return results in the order 
;; that Cassandra returned them.
(deftest time-uuid-column-comparator 
  (with-test-keyspace keyspace [{:name "A" :comparator :time-uuid}]
    (let [column-family "A"
          opts [:n-serializer :uuid
                :v-serializer :long]
          a (t/from-string "9e296c00-fec6-11e1-81e5-e0f84733a7f4")
          b (t/from-string "9c4462e0-fec7-11e1-81e5-e0f84733a7f4")]
      (put 
       keyspace 
       column-family 
       "row-key" 
       {a 4800
        b 5507})
      (is (= [(t/get-date a) (t/get-date b)]
             (-> (apply get-rows keyspace column-family ["row-key"] opts)
                 (first)
                 vals
                 first
                 keys
                 (#(map t/get-date %))))))))

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

(deftest get-rows-via-cql-query
  (with-test-keyspace keyspace [{:name "A"}]
    (let [column-family "A"
          opts [:v-serializer :string
                :n-serializer :string
                :k-serializer :string]]
      (put keyspace column-family "row-key" {"k" "v"})
      (is (= {"row-key" {"k" "v"}}
             (first (apply get-rows-cql-query keyspace "select * from A" opts)))))))

(deftest empty-result
  (with-test-keyspace keyspace [{:name "A"}]
    (let [column-family "A"
          opts [:v-serializer :string
                :n-serializer :string
                :k-serializer :string]]
      (is (= [{"row-key" {}}]
             (apply get-rows keyspace column-family ["row-key"] opts))))))

(deftest empty-result-via-cql-query
  (with-test-keyspace keyspace [{:name "A"}]
    (let [column-family "A"
          opts [:v-serializer :string
                :n-serializer :string
                :k-serializer :string]]
      (is (= {}
             (apply get-rows-cql-query keyspace "select * from A" opts))))))

(deftest dynamic-composite-column-ranges
  (with-test-keyspace keyspace [{:name "A"}]
    (let [column-family "A"
          opts [:v-serializer :string
                :n-serializer :dynamic-composite]]

      (let [cols (into {}
                       (interleave
                        (doseq [i (range 0 3)]
                          (create-dynamic-composite {:value "col"
                                                     :n-serializer :string
                                                     :comparator :utf-8}
                                                    {:value i
                                                     :n-serializer :integer
                                                     :comparator :integer}))
                        (cycle "v")))]
        (put keyspace column-family "row-key" cols)
        (is (= cols
               (apply get-column-range
                      keyspace
                      column-family
                      "row-key"
                      (first (keys cols))
                      (last (keys cols))
                      opts)))))))

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
        opts [:s-serializer :string :n-serializer :string :v-serializer :string :type :super]]
    (with-test-keyspace keyspace [{:name column-family :type :super}]
      (put keyspace column-family "row-key" {"SuperCol" {"n" "v" "n2" "v2"}
                                             "SuperCol2" {"n" "v" "n2" "v2"}} :type :super)
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
                                               "SuperCol2" {"n" "v"}} :type :super)
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
        (put keyspace column-family pk {"n" 1 "n2" 2} :counter true)
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
        (put keyspace column-family pk {"SuperCol" {"n" 1 "n2" 2}} :type :super :counter true)
        (is (= {"SuperCol" {"n" 1 "n2" 2}}
               (apply get-counter-super-columns keyspace column-family pk "SuperCol" ["n" "n2"] opts))))))
  (testing "counter column range query"
    (let [column-family "A"
          opts [:n-serializer :string :v-serializer :long]
          pk "row-key"]
      (with-test-keyspace keyspace [{:name column-family
                                     :validator :counter}]
        (put keyspace column-family "row-key" {"a" 1 "b" 2 "c" 1 "d" 3} :counter true)
        (is (= {"a" 1 "b" 2 "c" 1}
               (apply get-counter-column-range keyspace column-family pk "a" "c" opts)))))))

(deftest ttl-columns
  (testing "regular column ttl"
    (let [column-family "A"
          opts [:n-serializer :string :v-serializer :long]
          pk "row-key1"]
      (with-test-keyspace keyspace [{:name column-family}]
        (put keyspace column-family pk {"n" 1 "n2" 2} :ttl 1)
        (is (= {"n" 1 "n2" 2}
               (apply get-columns keyspace column-family pk ["n" "n2"] opts)))
        (Thread/sleep 2000)
        (is (= {}
               (apply get-columns keyspace column-family pk ["n" "n2"] opts))))))
  (testing "super column ttl"
    (let [column-family "A"
          opts [:s-serializer :string :n-serializer :string :v-serializer :long]
          pk "row-key"]
      (with-test-keyspace keyspace [{:name column-family
                                     :type :super}]
        (put keyspace column-family pk {"SuperCol" {"n" 1 "n2" 2}} :ttl 1 :type :super)
        (is (= {"n" 1 "n2" 2}
               (apply get-super-columns keyspace column-family pk "SuperCol" ["n" "n2"] opts)))
        (Thread/sleep 2000)
        (is (= {}
               (apply get-super-columns keyspace column-family pk "SuperCol" ["n" "n2"] opts)))))))

(deftest test-batch-put
  (let [column-family "A"
        opts [:s-serializer :string :n-serializer :string :v-serializer :long]
        pk1 "row-key1"
        pk2 "row-key2"]
    (with-test-keyspace keyspace [{:name column-family
                                   :type :super}]
      (batch-put keyspace column-family
                 {pk1 {"SuperCol" {"n" 1 "n2" 2}}
                  pk2 {"SuperCol" {"n3" 3 "n4" 4}}}
                 :ttl 1 :type :super)
      (is (= {"n" 1 "n2" 2}
             (apply get-super-columns keyspace column-family pk1
                    "SuperCol" ["n" "n2"] opts)))
      (is (= {"n3" 3 "n4" 4}
             (apply get-super-columns keyspace column-family pk2
                    "SuperCol" ["n3" "n4"] opts))))))

