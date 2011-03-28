(ns clj-hector.test.core
  (:use [clj-hector.core] :reload)
  (:use [clojure.test])
  (:import [me.prettyprint.cassandra.serializers StringSerializer]))

(deftest serializer-lookup
  (is (instance? StringSerializer
                 (serializer "Hello"))))
