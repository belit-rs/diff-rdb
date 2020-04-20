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
      (let [e (agent [])
            c (impl/reducible->chan
               (map #(/ (count %) 0))
               (impl/reducible-lines f)
               #(->> (Throwable->map %)
                     (send e conj)))]
        (is (drained? c))
        (await e)
        (is (= (map #(:cause %) @e)
               ["Divide by zero"]))))))
