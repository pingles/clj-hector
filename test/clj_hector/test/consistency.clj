(ns clj-hector.test.consistency
  (:use [clj-hector.consistency]
        [clojure.test])
  (:import [me.prettyprint.hector.api HConsistencyLevel]
           [me.prettyprint.cassandra.service OperationType]))

(deftest test-consistency-policy
  (are [policy-map type level] (=
                                 (do
                                   (.get (policy policy-map) type))
                                  level)
    {"*" {:read :one}} OperationType/READ (consistencies :one)
    {"*" {:read :one}} OperationType/WRITE (consistencies :one)
    {"*" {:read :quorum}} OperationType/READ (consistencies :quorum)
    {"*" {:read :quorum}} OperationType/WRITE (consistencies :one)))

(deftest test-consistency-policy-with-cf
  (are [policy-map type cf level] (= (.get (policy policy-map) type cf) level)
    {"*" {:read :one} "cf1" {:read :quorum}} OperationType/READ "cf1" (consistencies :quorum)
    {"*" {:read :one} "cf1" {:read :quorum}} OperationType/READ "*" (consistencies :one)
    {"*" {:read :one} "cf1" {:read :quorum}} OperationType/READ "fake" (consistencies :one)))

