(ns clj-hector.test.serialize
  (:use [clj-hector.serialize] :reload
        [clojure.test]))

;; Clojure 1.3 changes to storing long-or-smaller as longs.
;; http://dev.clojure.org/jira/browse/CLJ-820
;; Need to Integer/valueOf the value to make sure it returns
;; an int.
(deftest integer-serializer
  (let [s (serializer :integer)]
    (is (= 5
           (.fromBytes s (.toBytes s (Integer/valueOf 5)))))))

(deftest long-serializer
  (let [s (serializer :long)]
    (is (= (long 5) (.fromBytes s (.toBytes s (long 5)))))))

(deftest keyword-serialization
  (let [s (serializer :keyword)]
    (is (= :kw (.fromBytes s (.toBytes s :kw))))))

(deftest bad-serializer-name
  (is (thrown? NullPointerException (serializer :bad-serializer-keyword)))
  (try
    (serializer :bad-serializer-keyword)
    (is false "Should have gotten a NPE")
    (catch NullPointerException e
      (is (= ":bad-serializer-keyword did not resolve to a serializer." (.getMessage e))))))
