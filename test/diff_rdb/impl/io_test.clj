;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io-test
  (:require
   [clojure.test :refer :all]
   [clojure.string :as str]
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [diff-rdb.impl.io :as impl]))


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
  (nil? (async/<!! chan)))


(defn closed?
  [chan]
  (not (async/>!! chan ::check)))


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
        (is (closed?  c))
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
      (is (closed?  c))
      (is (= (into [] (map #(into {} %)) r)
             [{:column1 1}
              {:column1 2}
              {:column1 3}]))))
  (testing "Exception"
    (with-file [f (create-file)]
      (let [a (atom [])
            c (impl/reducible->chan
               (map #(/ (count %) 0))
               (impl/reducible-lines f)
               #(swap! a conj %))]
        (is (drained? c))
        (is (closed?  c))
        (is (= (map #(:cause %) @a)
               ["Divide by zero"]))))))


(deftest into-unordered-test
  (testing "Parallelism"
    (let [n       (.availableProcessors
                   (Runtime/getRuntime))
          coll    (set (range n))
          ch-to   (async/chan n)
          ch-from (async/chan)]
      (impl/into-unordered n
                           ch-to
                           #(do (Thread/sleep 25) %)
                           ch-from
                           #(throw (AssertionError.)))
      (async/onto-chan ch-from coll)
      (dotimes [_ n]
        (is (contains? coll (async/<!! ch-to))))
      (is (drained? ch-to))
      (is (closed?  ch-to))))
  (testing "Error handling"
    (let [ch-to    (async/chan)
          ch-from  (async/chan)
          ch-error (async/chan)]
      (impl/into-unordered 1
                           ch-to
                           #(/ % 0)
                           ch-from
                           #(async/put! ch-error %))
      (async/>!! ch-from 5)
      (is (= (:capture (async/<!! ch-error)) 5))
      (async/close! ch-from)
      (is (drained? ch-to))
      (is (closed?  ch-to))
      (async/close! ch-error))))
