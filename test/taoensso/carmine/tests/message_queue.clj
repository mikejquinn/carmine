(ns taoensso.carmine.tests.message-queue
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.message-queue :as mq]))

(comment (test/run-tests '[taoensso.carmine.tests.message-queue]))

(def tq :carmine-test-queue)
(defn- clear-tq [] (mq/clear-queues {} tq))
;; (defn- before-run {:expectations-options :before-run} [] (clear-tq))
;; (defn- after-run  {:expectations-options :after-run}  [] (clear-tq))

(defmacro wcar* [& body] `(car/wcar {} ~@body))
(defn- dequeue* [qname & [opts]]
  (let [r (mq/dequeue qname (merge {:eoq-backoff-ms 175} opts))]
    (Thread/sleep 205) r))

(def last-n_ (atom nil))
(defmacro debug [n & body]
  `(let [last-n# @last-n_]
     (reset! last-n_ ~n)
     (println (str last-n# "→" ~n))
     ~@body))

;;;; Basic enqueuing & dequeuing
(expect "eoq-backoff"    (debug 1 (do (clear-tq) (wcar {} (dequeue* tq)))))
(expect "mid1"           (debug 2 (wcar {} (mq/enqueue tq :msg1 :mid1))))
(expect {:messages {"mid1" :msg1}, :mid-circle ["mid1" "end-of-circle"]}
  (debug 3 (in (mq/queue-status {} tq))))
(expect :queued                     (debug 4 (wcar {} (mq/message-status tq :mid1))))
(expect {:carmine.mq/error :queued} (debug 5 (wcar {} (mq/enqueue tq :msg1 :mid1)))) ; Dupe
(expect "eoq-backoff"    (debug 6 (wcar {} (dequeue* tq))))
(expect ["mid1" :msg1 1] (debug 7 (wcar {} (dequeue* tq)))) ; New msg

(expect :locked          (debug 8 (wcar {} (mq/message-status tq :mid1))))
(expect "eoq-backoff"    (debug 9 (wcar {} (dequeue* tq))))
(expect nil              (debug 10 (wcar {} (dequeue* tq)))) ; Locked msg
