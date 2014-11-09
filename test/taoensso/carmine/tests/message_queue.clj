(ns taoensso.carmine.tests.message-queue
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.message-queue :as mq]))

(comment (test/run-tests '[taoensso.carmine.tests.message-queue]))

(defn- before-run {:expectations-options :before-run} [] (println "Before-run"))
(defn- after-run  {:expectations-options :after-run}  [] (println "After-run"))

(def last-n_ (atom 0))
(defmacro run-test-body [n & body]
  `(let [n#      ~n
         last-n# @last-n_]
     (reset! last-n_ n#)
     (println (str "Running " last-n# "→" n#))
     (Thread/sleep 80)
     ~@body))

;; (expect :foo (run-test-body 1 :foo))
;; (expect :foo (run-test-body 2 :foo))
;; (expect :foo (run-test-body 3 :foo))
;; (expect :foo (run-test-body 4 :foo))
;; (expect :foo (run-test-body 5 :foo))
;; =>
;; Before-run
;; Running 0→1
;; Running 1→2
;; Running 2→3
;; Running 3→4
;; Running 4→5
;; After-run


(expect :foo (run-test-body 1 :foo))
(expect :foo (run-test-body 2 :foo))
(expect :foo (run-test-body 3 :bar))
(expect :foo (run-test-body 4 :foo))
(expect :foo (run-test-body 5 :foo))
