;; Copyright (c) Belit d.o.o.


(ns diff-rdb.dev
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.spec.test.alpha :as st]
   [clojure.core.async :as async]
   [expound.alpha :as expound]
   [next.jdbc.specs]
   [diff-rdb.spec]))


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
                 ;; dev util
                 diff-rdb.dev]]
      (in-ns ns)
      (set! *warn-on-reflection* true)))


(defn is*
  [results]
  (assert (seq results))
  (clojure.test/is
   (if (some :failure results)
     (expound/explain-results results)
     true)))


(defmacro with-file
  [bindings & body]
  (assert (= (count bindings) 2))
  `(let ~bindings
     (try
       ~@body
       (finally
         (io/delete-file
          ~(bindings 0))))))


(defn create-file
  []
  (let [f (io/file "foo.txt")]
    (->> ["foo" "bar" "baz"]
         (str/join \newline)
         (spit f))
    f))


(defn db-spec
  []
  {:dbtype   "postgresql"
   :host     "localhost"
   :port     5432
   :dbname   "postgres"
   :user     "test"
   :password "test"})


(defn drained?
  [chan]
  (and (nil? (async/<!! chan))
       (not  (async/>!! chan ::check))))
