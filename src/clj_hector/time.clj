(ns clj-hector.time
  (:import [me.prettyprint.cassandra.utils TimeUUIDUtils]))

(defn uuid-now
  []
  (TimeUUIDUtils/getUniqueTimeUUIDinMillis))

(defn to-bytes
  [uuid]
  (TimeUUIDUtils/asByteArray uuid))

(defn from-bytes
  [bytes]
  (TimeUUIDUtils/toUUID bytes))

(defn get-date
  "Retrieves the Date represented by the bytes of a TimeUUID instance."
  [bytes]
  (java.util.Date. (TimeUUIDUtils/getTimeFromUUID bytes)))
