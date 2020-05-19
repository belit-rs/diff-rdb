;; Copyright (c) Belit d.o.o.


(ns diff-rdb.io-test
  (:require
   [clojure.test :refer :all]
   [clojure.core.async :as async]
   [diff-rdb.io :as io]))


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


(deftest ptn-test
  (testing "Standard case"
    (let [config {:ptn/size  3
                  :ptn/con   (db-spec)
                  :ptn/query "SELECT * FROM
                              (VALUES
                               (1), (2), (3), (4),
                               (5), (6), (7), (8)) AS _"}
          ch-err (async/chan)
          ch-ptn (io/ptn config ch-err)]
      (is (= (async/<!! ch-ptn) [1 2 3]))
      (is (= (async/<!! ch-ptn) [4 5 6]))
      (is (= (async/<!! ch-ptn) [7 8 7]))
      (is (drained? ch-ptn))
      (async/close! ch-err)
      (is (drained? ch-err))))
  (testing "Error handling"
    (let [config {:ptn/size  3
                  :ptn/con   (db-spec)
                  :ptn/query "SELECT * FROM_ERR
                              (VALUES
                               (1), (2), (3), (4),
                               (5), (6), (7), (8)) AS _"}
          ch-err (async/chan)
          ch-ptn (io/ptn config ch-err)]
      (is (drained? ch-ptn))
      (let [err (async/<!! ch-err)]
        (is (= (:err err) :ptn))
        (is (= (-> err :ex :via first :type)
               'org.postgresql.util.PSQLException)))
      (async/close! ch-err)
      (is (drained? ch-err)))))


(deftest diff-test
  (testing "Without partitioning"
    (let [config  {:match-by  [:c1]
                   :ponders   {:c2 3 :c3 2}
                   :workers   2
                   :src/con   (db-spec)
                   :tgt/con   (db-spec)
                   :src/query "SELECT * FROM
                                (VALUES (1, 3, 5, 6),
                                        (2, 7, 9, 8))
                                AS _ (c1, c2, c3, c4)"
                   :tgt/query "SELECT * FROM
                                (VALUES (2, 2, 9, 4),
                                        (3, 6, 7, 8))
                                AS _ (c1, c2, c3, c4)"}
          ch-err  (async/chan)
          ch-ptn  (async/to-chan! [[]])
          ch-diff (io/diff config ch-err ch-ptn)]
      (is (= (async/<!! (async/into #{} ch-diff))
             #{{:ins [{:c1 1 :c2 3 :c3 5 :c4 6}]}
               {:del [{:c1 3 :c2 6 :c3 7 :c4 8}]}
               {:upd [{:src  {:c1 2 :c2 7 :c3 9 :c4 8}
                       :tgt  {:c1 2 :c2 2 :c3 9 :c4 4}
                       :cols [:c2]}]}}))
      (async/close! ch-err)
      (is (drained? ch-err))))
  (testing "With partitioning"
    (let [config  {:match-by  [:c1]
                   :ponders   {:c2 3 :c3 2}
                   :workers   2
                   :src/con   (db-spec)
                   :tgt/con   (db-spec)
                   :src/query "SELECT * FROM
                                (VALUES (1, 3, 5, 6),
                                        (2, 7, 9, 8))
                                AS _ (c1, c2, c3, c4)
                               WHERE c1 IN (?, ?, ?)"
                   :tgt/query "SELECT * FROM
                                (VALUES (2, 2, 9, 4),
                                        (3, 6, 7, 8))
                                AS _ (c1, c2, c3, c4)
                               WHERE c1 IN (?, ?, ?)"}
          ch-err  (async/chan)
          ch-ptn  (async/to-chan! [[1 2 3] [4 4 4]])
          ch-diff (io/diff config ch-err ch-ptn)]
      (is (= (async/<!! (async/into #{} ch-diff))
             #{{:ins [{:c1 1 :c2 3 :c3 5 :c4 6}]}
               {:del [{:c1 3 :c2 6 :c3 7 :c4 8}]}
               {:upd [{:src  {:c1 2 :c2 7 :c3 9 :c4 8}
                       :tgt  {:c1 2 :c2 2 :c3 9 :c4 4}
                       :cols [:c2]}]}}))
      (async/close! ch-err)
      (is (drained? ch-err))))
  (testing "Error handling"
    (let [config  {:match-by  [:c1]
                   :ponders   {:c2 3 :c3 2}
                   :workers   2
                   :src/con   (db-spec)
                   :tgt/con   (db-spec)
                   :src/query "SELECT * FROM
                               (VALUES (1)) AS _ (c1)
                               WHERE c1 IN (?)"
                   :tgt/query "SELECT * FROM_ERR
                               (VALUES (2)) AS _ (c1)
                               WHERE c1 IN (?)"}
          ch-err  (async/chan)
          ch-ptn  (async/to-chan! [[1]])
          ch-diff (io/diff config ch-err ch-ptn)]
      (let [err (async/<!! ch-err)]
        (is (= (:err err) :parallel-select))
        (is (= (:ptn err) [1]))
        (is (= (-> err :ex :via first :type)
               'org.postgresql.util.PSQLException)))
      (async/close! ch-err)
      (is (drained? ch-err))
      (is (drained? ch-diff)))))
