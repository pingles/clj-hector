(ns ^{:author "Paul Ingles"
      :description "Utility functions for dealing with time values, such as TimeUUID instances."}
  clj-hector.time
  (:import [me.prettyprint.cassandra.utils TimeUUIDUtils]
           [java.util Date UUID]
           [org.joda.time ReadableInstant]))

(defn uuid-now
  []
  (TimeUUIDUtils/getUniqueTimeUUIDinMillis))

(defprotocol ToEpoch
  (epoch [_]
    "Returns the milliseconds since epoch. Epoch can be either
     a java.util.Date instance, or an org.joda.time.ReadableInstant"))

(extend-protocol ToEpoch
  Date (epoch [d] (.getTime d))
  ReadableInstant (epoch [i] (.getMillis i)))

(defn uuid
  "Creates a UUID from an epoch value"
  [time]
  (TimeUUIDUtils/getTimeUUID time))

(defn to-bytes
  "Converts a TimeUUID object to a byte array suitable for serializing."
  [uuid]
  (TimeUUIDUtils/asByteArray uuid))

(defn from-bytes
  "Deserializes a TimeUUID object from a byte array."
  [bytes]
  (TimeUUIDUtils/toUUID bytes))

(defn- byte-array?
  [obj]
  (= Byte/TYPE (.getComponentType (class obj))))

(defmulti ^{:arglists '([object])} get-date
  "Retrieves the date from a TimeUUID object. TimeUUID can be provided as either
   a UUID instance, or serialized as a byte array."
  (fn [obj] (if (byte-array? obj) :bytes :uuid)))
(defmethod get-date :uuid [uuid] (get-date (to-bytes uuid)))
(defmethod get-date :bytes [bytes] (Date. (TimeUUIDUtils/getTimeFromUUID bytes)))


