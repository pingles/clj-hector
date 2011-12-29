(ns clj-hector.test.test-helper
  (:use [clojure.test]))

;; A Simple macro that enable to mark your test
;; to pending
;; Taken from: http://techbehindtech.com/2010/06/01/marking-tests-as-pending-in-clojure/
(defmacro deftest-pending
  [name & body]
   (let [message (str "\n========\n" name " is pending !!\n========\n")]
     `(deftest ~name
        (println ~message))))
