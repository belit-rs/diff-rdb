;; Copyright (c) Belit d.o.o.


(ns diff-rdb.dev
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [orchestra.spec.test :as st]
   [clojure.core.async :as async]
   [expound.alpha :as expound]
   [clojure.test]
   [next.jdbc.specs]
   [diff-rdb.spec])
  (:import (java.util UUID)))


(defn is*
  [results]
  (assert (seq results))
  (clojure.test/is
   (if (some :failure results)
     (expound/explain-results results)
     true)))


(defmacro with-err-str
  [& body]
  `(let [s# (new java.io.StringWriter)]
     (binding [*err* s#]
       ~@body
       (str s#))))


(defmacro with-file
  [bindings & body]
  (assert (= (count bindings) 2))
  `(let ~bindings
     (try
       ~@body
       (finally
         (io/delete-file
          ~(bindings 0))))))


(defmacro thrown-uncaught?
  [c expr]
  `(let [p# (promise)]
     (try (Thread/setDefaultUncaughtExceptionHandler
           (reify Thread$UncaughtExceptionHandler
             (uncaughtException [_ _ ex#]
               (assert (not (realized? p#)))
               (deliver p# (type ex#)))))
          (let [expr# ~expr]
            (= ~c (deref p# 1000 (UUID/randomUUID))))
          (finally
            (Thread/setDefaultUncaughtExceptionHandler nil)))))


(defn create-file
  []
  (let [f (io/file "foo.txt")]
    (->> ["foo" "bar" "baz"]
         (str/join \newline)
         (spit f))
    f))


(defn db-spec
  []
  {:dbtype "h2"
   :dbname "test"
   :mode   "PostgreSQL"})


(defn drained?
  [chan]
  (and (nil? (async/<!! chan))
       (not  (async/>!! chan ::check))))


(do (set! s/*explain-out* expound/printer)
    (s/check-asserts true)
    (st/instrument)
    (doseq [ns '[;; impl
                 diff-rdb.impl.diff
                 diff-rdb.impl.io
                 diff-rdb.impl.util
                 ;; api
                 diff-rdb.diff
                 diff-rdb.io
                 diff-rdb.spec
                 ;; test impl
                 diff-rdb.impl.diff-test
                 diff-rdb.impl.io-test
                 diff-rdb.impl.util-test
                 ;; test api
                 diff-rdb.diff-test
                 diff-rdb.io-test
                 diff-rdb.doc.api-test
                 ;; dev util
                 diff-rdb.dev]]
      (require ns)
      (in-ns ns)
      (set! *warn-on-reflection* true)))


(comment

  (clojure.test/run-all-tests #"diff-rdb.*.-test")

  ,)
