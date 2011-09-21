(ns clj-hector.time
  (:import [me.prettyprint.cassandra.utils TimeUUIDUtils]
           [java.util Date UUID]
           [org.joda.time ReadableInstant]))

(defn uuid-now
  []
  (TimeUUIDUtils/getUniqueTimeUUIDinMillis))

(defprotocol ToEpoch
  (epoch [_] "Returns the milliseconds since epoch"))

(extend-protocol ToEpoch
  Date (epoch [d] (.getTime d))
  ReadableInstant (epoch [i] (.getMillis i)))

(defn uuid
  "Creates a UUID from an epoch value"
  [time]
  (TimeUUIDUtils/getTimeUUID time))

(defn to-bytes
  [uuid]
  (TimeUUIDUtils/asByteArray uuid))

(defn from-bytes
  [bytes]
  (TimeUUIDUtils/toUUID bytes))

(defn- byte-array?
  [obj]
  (= Byte/TYPE (.getComponentType (class obj))))

(defmulti get-date (fn [obj] (if (byte-array? obj) :bytes :uuid)))
(defmethod get-date :uuid [uuid] (get-date (to-bytes uuid)))
(defmethod get-date :bytes [bytes] (Date. (TimeUUIDUtils/getTimeFromUUID bytes)))


