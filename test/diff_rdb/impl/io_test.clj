;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io-test
  (:require
   [clojure.test :refer [deftest testing is are]]
   [clojure.string :as str]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [diff-rdb.impl.io :as impl]
   [diff-rdb.dev :refer [with-err-str
                         with-file
                         thrown-uncaught?
                         create-file
                         drained?
                         db-spec]])
  (:import
   (java.io File IOException)
   (java.lang ArithmeticException)
   (clojure.lang ExceptionInfo)
   (org.postgresql.util PSQLException)))


(deftest <??-test
  (is (thrown?
       ArithmeticException
       (impl/<?? (async/thread
                   (try (/ 2 0)
                        (catch Throwable e e))))))
  (is (zero? (impl/<?? (async/thread 0)))))


(deftest reducible-lines-test
  (with-file [f (create-file)]
    (let [r (impl/reducible-lines f)]
      (is (= (into [] (map str/upper-case) r)
             (into [] (map str/upper-case) r)
             ["FOO" "BAR" "BAZ"]))))
  (is (impl/reducible-lines nil))
  (is (thrown?
       IllegalArgumentException
       (into []
             (map str/upper-case)
             (impl/reducible-lines nil)))))


(deftest uncaught-ex-chan-test
  (try
    (let [chan   (async/chan 1 (map #(/ 1 %)))
          ch-err (impl/uncaught-ex-chan)]
      (async/>!! chan 0)
      (async/>!! chan 0)
      (is (= (type (async/<!! ch-err)) ArithmeticException))
      (is (= (type (async/<!! ch-err)) ArithmeticException))
      (async/>!! chan 0)
      (async/close! ch-err)
      (is (= (type (async/<!! ch-err)) ArithmeticException))
      (is (drained? ch-err))
      (is (-> (with-err-str (async/>!! chan 0))
              (str/includes? "ArithmeticException")))
      (async/close! chan)
      (is (drained? chan)))
    (finally
      (Thread/setDefaultUncaughtExceptionHandler nil))))


(deftest reducible->chan-test
  (testing "File"
    (with-file [f (create-file)]
      (let [r (impl/reducible-lines f)
            c (impl/reducible->chan
               (map str/upper-case) r)]
        (is (= (async/<!! c) "FOO"))
        (is (= (async/<!! c) "BAR"))
        (is (= (async/<!! c) "BAZ"))
        (is (drained? c))
        (is (= (into [] (map str/upper-case) r)
               ["FOO" "BAR" "BAZ"])))))
  (testing "Database"
    (let [r (jdbc/plan
             (db-spec)
             ["SELECT column1
                 FROM (VALUES (1, 1, 1),
                              (2, 2, 2),
                              (3, 3, 3)) AS _"])
          c (impl/reducible->chan
             (map #(into {} %)) r)]
      (is (= (async/<!! c) {:column1 1}))
      (is (= (async/<!! c) {:column1 2}))
      (is (= (async/<!! c) {:column1 3}))
      (is (drained? c))
      (is (= (into [] (map #(into {} %)) r)
             [{:column1 1}
              {:column1 2}
              {:column1 3}]))))
  (testing "Exception"
    (with-file [f (create-file)]
      (is (thrown-uncaught?
           ArithmeticException
           (impl/reducible->chan
            (map #(/ (count %) 0))
            (impl/reducible-lines f))))))
  (testing "Resource management"
    (let [f (create-file)
          _ (impl/reducible-lines f)]
      (is (io/delete-file f)))
    (let [f (create-file)
          r (impl/reducible-lines f)
          c (impl/reducible->chan
             (map str/upper-case) r)]
      (is (= (async/<!! c) "FOO"))
      (is (thrown?
           IOException
           (io/delete-file f)))
      (async/close! c)
      (is (thrown?
           IOException
           (io/delete-file f)))
      (is (= (async/<!! c) "BAR"))
      (is (drained? c))
      (Thread/sleep 50)
      (is (io/delete-file f)))))


(deftest run-in-pool-test
  (is (thrown?
       AssertionError
       (impl/run-in-pool -1 nil nil nil)))
  (is (thrown?
       AssertionError
       (impl/run-in-pool 0 nil nil nil)))
  (are [args]
      (try
        (let [{:keys [n in out err]} args
              ch-in  (async/to-chan! in)
              ch-out (async/chan)
              ch-err (impl/uncaught-ex-chan)
              pout   (promise)
              perr   (promise)]
          (async/go-loop [vs #{}]
            (if-some [v (async/<! ch-out)]
              (recur (conj vs v))
              (deliver pout vs)))
          (async/go-loop [vs {}]
            (if-some [^Throwable v (async/<! ch-err)]
              (recur (update vs
                             (-> v .getClass .getSimpleName)
                             (fnil inc 0)))
              (deliver perr vs)))
          (impl/run-in-pool n
                            (fn f []
                              (when-some [v (async/<!! ch-in)]
                                (async/>!! ch-out (/ 1 v))
                                (recur)))
                            (constantly true)
                            (fn on-close []
                              (async/close! ch-out)
                              (async/close! ch-err)))
          (is (= out (deref pout 5000 ::timeout)))
          (is (= err (deref perr 5000 ::timeout)))
          (is (drained? ch-in))
          (is (drained? ch-out))
          (is (drained? ch-err)))
        (finally (Thread/setDefaultUncaughtExceptionHandler nil)))

    {:n 1 :in [] :out #{} :err {}}
    {:n 3 :in [] :out #{} :err {}}

    {:n   1
     :in  [1 2 0 0 3 0 0 0 0 4 "asd"]
     :out #{1/1 1/2 1/3 1/4}
     :err {"ArithmeticException" 6
           "ClassCastException"  1}}

    {:n   3
     :in  [1 2 0 0 3 0 0 0 0 4 "asd"]
     :out #{1/1 1/2 1/3 1/4}
     :err {"ArithmeticException" 6
           "ClassCastException"  1}}

    {:n   1
     :in  (conj (repeat 100 0) 5)
     :out #{1/5}
     :err {"ArithmeticException" 100}}

    {:n   3
     :in  (conj (repeat 9000 0) 5 6)
     :out #{1/5 1/6}
     :err {"ArithmeticException" 9000}}))


(deftest async-select-test
  (let [sql-params ["SELECT column1
                       FROM (VALUES (1, 1, 1),
                                    (2, 2, 2),
                                    (3, 3, 3)) AS _
                      WHERE column3 IN (?, ?)"]]
    (with-open [con (jdbc/get-connection (db-spec))
                pst (jdbc/prepare con sql-params)]
      (is (= (async/<!! (impl/async-select pst [1 3] {}))
             [{:column1 1} {:column1 3}]))
      (is (= (async/<!! (impl/async-select pst [2 1] {}))
             [{:column1 1} {:column1 2}]))
      (is (thrown?
           PSQLException
           (impl/<?? (impl/async-select pst [1 2 3] {}))))))
  (let [sql-params ["SELECT * FROM (VALUES (1, 2)) AS _"]]
    (with-open [con (jdbc/get-connection (db-spec))
                pst (jdbc/prepare con sql-params)]
      (is (= (async/<!! (impl/async-select pst [] {}))
             [{:column1 1 :column2 2}])))))


(deftest parallel-select-fn-test
  (testing "Without partitions"
    (let [ch-ptn (async/to-chan! [[]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:conn  (db-spec)
                      :query "SELECT * FROM (VALUES (1)) AS _"}
                     {:conn  (db-spec)
                      :query "SELECT * FROM (VALUES (2)) AS _"}
                     ch-ptn ch-out)))]
      (is (= (async/<!! ch-out) [[{:column1 1}] [{:column1 2}]]))
      (is (nil? @select))
      (async/close! ch-out)))
  (testing "With partitions"
    (let [ch-ptn (async/to-chan! [[1] [2] [3]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:conn  (db-spec)
                      :query "SELECT column3
                                FROM (VALUES (1, 2, 3),
                                             (2, 3, 4),
                                             (3, 4, 5)) AS _
                               WHERE column1 = ?"}
                     {:conn  (db-spec)
                      :query "SELECT column3
                                FROM (VALUES (1, 2, 3),
                                             (2, 3, 4),
                                             (3, 4, 5)) AS _
                               WHERE column1 = ?"}
                     ch-ptn ch-out)))]
      (is (= (async/<!! ch-out) [[{:column3 3}] [{:column3 3}]]))
      (is (= (async/<!! ch-out) [[{:column3 4}] [{:column3 4}]]))
      (is (= (async/<!! ch-out) [[{:column3 5}] [{:column3 5}]]))
      (is (nil? @select))
      (async/close! ch-out)))
  (testing "Change options"
    (let [ch-ptn (async/to-chan! [[1] [2] [3]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:conn  (db-spec)
                      :query "SELECT column3
                                FROM (VALUES (1, 2, 3),
                                             (2, 3, 4),
                                             (3, 4, 5)) AS _
                               WHERE column1 = ?"
                      :opts  {:builder-fn rs/as-arrays}}
                     {:conn  (db-spec)
                      :query "SELECT column3
                                FROM (VALUES (1, 2, 3),
                                             (2, 3, 4),
                                             (3, 4, 5)) AS _
                               WHERE column1 = ?"}
                     ch-ptn ch-out)))]
      (is (= (async/<!! ch-out) [[[:column3] [3]] [{:column3 3}]]))
      (is (= (async/<!! ch-out) [[[:column3] [4]] [{:column3 4}]]))
      (is (= (async/<!! ch-out) [[[:column3] [5]] [{:column3 5}]]))
      (is (nil? @select))
      (async/close! ch-out)))
  (testing "Error handling"
    (let [ch-ptn (async/to-chan! [[1]])
          ch-out (async/chan)]
      (try ((impl/parallel-select-fn
             {:conn  (db-spec)
              :query "SELECT column3
                        FROM (VALUES (1, 2, 3),
                                     (2, 3, 4),
                                     (3, 4, 5)) AS _
                       WHERE column1 = ?"}
             {:conn  (db-spec)
              :query "SELECT_ERR column3
                        FROM (VALUES (1, 2, 3),
                                     (2, 3, 4),
                                     (3, 4, 5)) AS _
                       WHERE column1 = ?"}
             ch-ptn ch-out))
           (catch ExceptionInfo ex
             (is (instance? PSQLException (ex-cause ex)))
             (is (= (ex-data ex) {:ptn [1]}))))
      (async/close! ch-out)))
  (testing "Close ch-out"
    (let [ch-ptn (async/to-chan! [[1] [2] [3]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:conn  (db-spec)
                      :query "SELECT column3
                                FROM (VALUES (1, 2, 3),
                                             (2, 3, 4),
                                             (3, 4, 5)) AS _
                               WHERE column1 = ?"}
                     {:conn  (db-spec)
                      :query "SELECT column3
                                FROM (VALUES (1, 2, 3),
                                             (2, 3, 4),
                                             (3, 4, 5)) AS _
                               WHERE column1 = ?"}
                     ch-ptn ch-out)))]
      (is (= (async/<!! ch-out) [[{:column3 3}] [{:column3 3}]]))
      (async/close! ch-out)
      (async/poll! ch-out)
      (is (drained? ch-out))
      (is (nil? @select))
      (async/close! ch-ptn))))


(deftest drain-to-file-test
  (testing "Standard case"
    (with-file [f (io/file "foo.txt")]
      (let [d [{:foo 1 :bar 1 :baz 1}
               {:foo 2 :bar 2 :baz nil}]
            c (async/to-chan d)
            c (impl/drain-to-file f pr-str c)]
        (async/<!! c)
        (is (= (into
                []
                (map edn/read-string)
                (impl/reducible-lines f))
               d)))))
  (testing "Make parent dir"
    (let [p "foo"
          f (str p File/separator "bar.txt")]
      (try
        (let [d [{:foo 1 :bar 1 :baz 1}
                 {:foo 2 :bar 2 :baz nil}]
              c (async/to-chan d)
              c (impl/drain-to-file f pr-str c)]
          (async/<!! c)
          (is (= (into
                  []
                  (map edn/read-string)
                  (impl/reducible-lines f))
                 d)))
        (finally (io/delete-file f)
                 (io/delete-file p)))))
  (testing "Exception"
    (with-file [f (io/file "foo.txt")]
      (let [d [{:foo 1 :bar 1 :baz 1}
               {:foo 2 :bar 2 :baz nil}]
            c (async/to-chan d)]
        (is (thrown-uncaught?
             ClassCastException
             (impl/drain-to-file f inc c)))))))
