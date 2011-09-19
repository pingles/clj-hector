(ns clj-hector.test.time
  (:import [java.util UUID])
  (:use [clojure.test]
        [clj-hector.time] :reload))

(defn close-enough?
  [a b]
  (let [diff (- (.getTime a)
                (.getTime b))]
    (< diff 100)))

(deftest time-as-uuid
  (let [now (uuid-now)
        now-date (java.util.Date. )]
    (is (= now (from-bytes (to-bytes now))))
    (is (close-enough? now-date (get-date (to-bytes now))))
    (is (instance? UUID now))
    (is (= now (uuid now-date)))
    (is (close-enough? now-date
                       (get-date (to-bytes (uuid now-date)))))))
