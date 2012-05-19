(ns carmine.protocol
  "Facilities for actually communicating with Redis server using its
  request/response protocol. Originally adapted from Accession."
  (:require [clojure.string :as str]
            [carmine.utils  :as utils])
  (:import  [java.io DataInputStream BufferedOutputStream]))

;; Hack to allow cleaner separation of ns concerns
(utils/declare-remote carmine.connections/in-stream
                      carmine.connections/out-stream)

(def ^:dynamic *context*
  "For flexibility, our server protocol fns can be called either with explicit
  context provided through arguments OR with a thread-bound context.

  A valid context should consist of AT LEAST an in and out stream but may
  contain other useful adornments. For example:

      * Flags to control encoding/decoding (serialization, etc.).
      * A counter atom to allow us to keep track of how many commands we've sent
        to server since last asking for replies while pipelining. This allows
        'with-conn' to take arbitrary body forms for power & flexibility."
  nil)

(def ^:private no-context-error
  (Exception. (str "Redis commands must be executed within the context of a"
                   " connection to Redis server. See 'with-conn'.")))

(def ^:private ^:const charset     "UTF-8")
(def ^:private ^:const bytes-class (Class/forName "[B"))

(defn bytestring
  "Redis communicates with clients using a (binary-safe) byte string protocol.
  This is the equivalent of the byte array representation of a Java String.
  Binary safe: won't munge input that's already a byte array."
  ^bytes [x]
  (if (instance? bytes-class x) x ; For binary safety
      (.getBytes (str x) charset)))

;;; Request delimiters
(def ^bytes   bs-crlf (bytestring "\r\n"))
(def ^Integer bs-*    (int (first (bytestring "*"))))
(def ^Integer bs-$    (int (first (bytestring "$"))))

;;; Fns to actually send data to stream
(defn send-crlf [^BufferedOutputStream out] (.write out bs-crlf 0 2))
(defn send-*    [^BufferedOutputStream out] (.write out bs-*))
(defn send-$    [^BufferedOutputStream out] (.write out bs-$))
(defn send-arg
  "Send arbitrary argument along with information about its size:
  $<size of arg> crlf
  <arg data>     crlf"
  [^BufferedOutputStream out arg]
  (let [bs   (bytestring arg)
        size (int (count bs))]
    (send-$ out) (.write out (bytestring size)) (send-crlf out)
    (.write out bs 0 size) (send-crlf out)))

(defn send-request!
  "Sends a command to Redis server using its byte string protocol:

      *<no. of args>     crlf
      [ $<size of arg N> crlf
        <arg data>       crlf ...]

  Ref: http://redis.io/topics/protocol. If explicit context isn't provided,
  uses thread-bound *context*."
  [{:keys [out-stream] :as ?context} command-name & command-args]
  (let [context               (merge *context* ?context)
        ^BufferedOutputStream out (or (:out-stream context)
                                      (throw no-context-error))
        request-args (cons command-name command-args)]

    (send-* out)
    (.write out (bytestring (count request-args)))
    (send-crlf out)

    (dorun (map (partial send-arg out) request-args))
    (.flush out)

    ;; Keep track of how many responses are queued with server
    (when-let [c (:atomic-reply-count context)] (swap! c inc))))

(defn get-response!
  "BLOCKs to receive queued replies from Redis server. Parses and returns them.

  Redis will reply to commands with different kinds of replies. It is possible
  to check the kind of reply from the first byte sent by the server:

      * With a single line reply the first byte of the reply will be `+`
      * With an error message the first byte of the reply will be `-`
      * With an integer number the first byte of the reply will be `:`
      * With bulk reply the first byte of the reply will be `$`
      * With multi-bulk reply the first byte of the reply will be `*`

  Error replies will be parsed as exceptions. If only a single reply is received
  and it is an error, the exception will be thrown.

  If explicit context isn't provided, uses thread-bound *context*."
  [& {:keys [in-stream reply-count] :as ?context}]
  (let [context             (merge *context* ?context)
        ^DataInputStream in (or (:in-stream context)
                                (throw no-context-error))
        count (or (:reply-count context)
                  (when-let [c (:atomic-reply-count context)] @c)
                  0)

        get-reply!
        (fn get-reply! []
          (let [reply-type (char (.readByte in))]
            (case reply-type
              \+ (.readLine in)
              \- (Exception. (.readLine in))
              \: (Long/parseLong (.readLine in))
              \$ (let [data-length (Integer/parseInt (.readLine in))]
                   (when-not (neg? data-length)
                     (let [data (byte-array data-length)]
                       (.read in data 0 data-length)
                       (.skip in 2) ; Final crlf
                       (String. data charset))))
              \* (let [length (Integer/parseInt (.readLine in))]
                   (doall (repeatedly length get-reply!)))
              (throw (Exception. (str "Server returned unknown reply type: "
                                      reply-type))))))]

    (case (int count)
      0 nil
      1 (let [r (get-reply!)] (if (instance? Exception r) (throw r) r))
      (doall (repeatedly count get-reply!)))))

(defmacro with-context
  "Evaluates body with thread-bound IO streams."
  [connection & body]
  `(binding [*context*
             {:in-stream  (carmine.connections/in-stream  ~connection)
              :out-stream (carmine.connections/out-stream ~connection)}]
     ~@body))

(defmacro with-context-and-response
  "Evaluates body with thread-bound IO streams and returns the server's response."
  [connection & body]
  `(binding [*context*
             {:in-stream  (carmine.connections/in-stream  ~connection)
              :out-stream (carmine.connections/out-stream ~connection)
              :atomic-reply-count (atom 0)}]
     ~@body
     (get-response!)))