;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io-test
  (:require
   [clojure.test :refer :all]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [diff-rdb.impl.io :as impl])
  (:import
   (java.io IOException)))


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
                                 (.getClass)
                                 (.getSimpleName)
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
