;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io-test
  (:require
   [clojure.test :refer :all]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [diff-rdb.impl.io :as impl])
  (:import
   (java.io IOException)
   (java.lang ArithmeticException)
   (clojure.lang ExceptionInfo)
   (org.postgresql.util PSQLException)))


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


(deftest reducible->chan-test
  (testing "File"
    (with-file [f (create-file)]
      (let [r (impl/reducible-lines f)
            c (impl/reducible->chan
               (map str/upper-case) r (fn [_]))]
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
             (map #(into {} %)) r (fn [_]))]
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
      (let [e (promise)
            c (impl/reducible->chan
               (map #(/ (count %) 0))
               (impl/reducible-lines f)
               #(->> (Throwable->map %)
                     :cause (deliver e)))]
        (is (drained? c))
        (is (= (deref e 50 false)
               "Divide by zero")))))
  (testing "Resource management"
    (let [f (create-file)
          r (impl/reducible-lines f)]
      (is (io/delete-file f)))
    (let [f (create-file)
          r (impl/reducible-lines f)
          c (impl/reducible->chan
             (map str/upper-case)
             r (fn [_]))]
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


(deftest sink-chan-test
  (let [p (promise)
        c (impl/sink-chan -1 #(deliver p true))]
    (is (drained? c))
    (is (deref p 50 false)))
  (let [p (promise)
        c (impl/sink-chan 0 #(deliver p true))]
    (is (drained? c))
    (is (deref p 50 false)))
  (let [p (promise)
        c (impl/sink-chan 1 #(deliver p true))]
    (is (async/>!! c 1))
    (is (deref p 50 false))
    (is (drained? c)))
  (let [p (promise)
        c (impl/sink-chan 2 #(deliver p true))]
    (is (async/>!! c false))
    (is (async/>!! c 2))
    (is (deref p 50 false))
    (is (drained? c)))
  (let [p (promise)
        c (impl/sink-chan 2 #(deliver p true))]
    (async/close! c)
    (is (deref p 50 false))
    (is (drained? c))))


(deftest run-in-pool-test
  (is (thrown?
       AssertionError
       (impl/run-in-pool -1 nil nil nil)))
  (is (thrown?
       AssertionError
       (impl/run-in-pool 0 nil nil nil)))
  (are [args]
      (let [{:keys [n in out err]} args
            ch-in  (async/to-chan in)
            ch-out (async/chan)
            ch-err (async/chan)
            pout   (promise)
            perr   (promise)]
        (async/go-loop [vs #{}]
          (if-some [v (async/<! ch-out)]
            (recur (conj vs v))
            (deliver pout vs)))
        (async/go-loop [vs #{}]
          (if-some [v (async/<! ch-err)]
            (recur (conj vs v))
            (deliver perr vs)))
        (impl/run-in-pool n
                          (fn f []
                            (when-some [v (async/<!! ch-in)]
                              (async/>!! ch-out (/ 1 v))
                              (recur)))
                          (fn ex-handler [ex]
                            (->> ^Throwable ex
                                 .getClass
                                 .getSimpleName
                                 (async/>!! ch-err)))
                          (fn on-close []
                            (async/close! ch-out)
                            (async/close! ch-err)))
        (is (= out (deref pout 5000 ::timeout)))
        (is (= err (deref perr 5000 ::timeout)))
        (is (drained? ch-in))
        (is (drained? ch-out))
        (is (drained? ch-err)))

    {:n 1 :in [] :out #{} :err #{}}
    {:n 3 :in [] :out #{} :err #{}}

    {:n   1
     :in  [1 2 0 0 3 0 0 0 0 4 "asd"]
     :out #{1/1 1/2 1/3 1/4}
     :err #{"ArithmeticException"
            "ClassCastException"}}

    {:n   3
     :in  [1 2 0 0 3 0 0 0 0 4 "asd"]
     :out #{1/1 1/2 1/3 1/4}
     :err #{"ArithmeticException"
            "ClassCastException"}}

    {:n   1
     :in  (conj (repeat 100 0) 5)
     :out #{1/5}
     :err #{"ArithmeticException"}}

    {:n   3
     :in  (conj (repeat 9000 0) 5 6)
     :out #{1/5 1/6}
     :err #{"ArithmeticException"}}))


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
    (let [ch-ptn (async/to-chan [[]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:src/con (db-spec)
                      :tgt/con (db-spec)
                      :src/query "SELECT * FROM (VALUES (1)) AS _"
                      :tgt/query "SELECT * FROM (VALUES (2)) AS _"}
                     ch-ptn ch-out)))]
      (is (= (async/<!! ch-out) [[{:column1 1}] [{:column1 2}]]))
      (is (nil? @select))
      (async/close! ch-out)))
  (testing "With partitions"
    (let [ch-ptn (async/to-chan [[1] [2] [3]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:src/con (db-spec)
                      :tgt/con (db-spec)
                      :src/query "SELECT column3
                                    FROM (VALUES (1, 2, 3),
                                                 (2, 3, 4),
                                                 (3, 4, 5)) AS _
                                   WHERE column1 = ?"
                      :tgt/query "SELECT column3
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
    (let [ch-ptn (async/to-chan [[1] [2] [3]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:src/con (db-spec)
                      :tgt/con (db-spec)
                      :src/query "SELECT column3
                                    FROM (VALUES (1, 2, 3),
                                                 (2, 3, 4),
                                                 (3, 4, 5)) AS _
                                   WHERE column1 = ?"
                      :tgt/query "SELECT column3
                                    FROM (VALUES (1, 2, 3),
                                                 (2, 3, 4),
                                                 (3, 4, 5)) AS _
                                   WHERE column1 = ?"
                      :src/exe-opts {:builder-fn rs/as-arrays}}
                     ch-ptn ch-out)))]
      (is (= (async/<!! ch-out) [[[:column3] [3]] [{:column3 3}]]))
      (is (= (async/<!! ch-out) [[[:column3] [4]] [{:column3 4}]]))
      (is (= (async/<!! ch-out) [[[:column3] [5]] [{:column3 5}]]))
      (is (nil? @select))
      (async/close! ch-out)))
  (testing "Error handling"
    (let [ch-ptn (async/to-chan [[1]])
          ch-out (async/chan)]
      (try ((impl/parallel-select-fn
             {:src/con (db-spec)
              :tgt/con (db-spec)
              :src/query "SELECT column3
                            FROM (VALUES (1, 2, 3),
                                         (2, 3, 4),
                                         (3, 4, 5)) AS _
                           WHERE column1 = ?"
              :tgt/query "SELECT_ERR column3
                            FROM (VALUES (1, 2, 3),
                                         (2, 3, 4),
                                         (3, 4, 5)) AS _
                           WHERE column1 = ?"}
             ch-ptn ch-out))
           (catch ExceptionInfo ex
             (let [err (ex-data ex)]
               (is (= (:err err) :parallel-select))
               (is (= (:ptn err) [1]))
               (is (= (-> err :ex :via first :type)
                      'org.postgresql.util.PSQLException)))))
      (async/close! ch-out)))
  (testing "Close ch-out"
    (let [ch-ptn (async/to-chan [[1] [2] [3]])
          ch-out (async/chan)
          select (future
                   ((impl/parallel-select-fn
                     {:src/con (db-spec)
                      :tgt/con (db-spec)
                      :src/query "SELECT column3
                                    FROM (VALUES (1, 2, 3),
                                                 (2, 3, 4),
                                                 (3, 4, 5)) AS _
                                   WHERE column1 = ?"
                      :tgt/query "SELECT column3
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
