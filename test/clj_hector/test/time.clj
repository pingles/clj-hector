(ns clj-hector.test.time
  (:import [java.util UUID])
  (:use [clojure.test]
        [clj-hector.time] :reload))

(deftest time-as-uuid
  (let [now (uuid-now)
        now-date (java.util.Date. )]
    (is (= now
           (from-bytes (to-bytes now))))
    (is (= now-date
           (get-date (to-bytes now))))
    (is (instance? UUID now))))
