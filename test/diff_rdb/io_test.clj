;; Copyright (c) Belit d.o.o.


(ns diff-rdb.io-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [clojure.core.async :as async]
   [diff-rdb.io :as io]
   [diff-rdb.impl.io :as impl]
   [diff-rdb.dev :refer [thrown-uncaught?
                         drained?
                         db-spec]])
  (:import
   (org.postgresql.util PSQLException)))


(deftest ptn-test
  (testing "Standard case"
    (let [config {:ptn/size 3
                  :ptn/plan {:conn  (db-spec)
                             :query "SELECT * FROM
                                      (VALUES
                                       (1), (2), (3), (4),
                                       (5), (6), (7), (8)) AS _"}}
          ch-ptn (io/ptn config)]
      (is (= (async/<!! ch-ptn) [1 2 3]))
      (is (= (async/<!! ch-ptn) [4 5 6]))
      (is (= (async/<!! ch-ptn) [7 8 7]))
      (is (drained? ch-ptn))))
  (testing "Error handling"
    (let [config {:ptn/size 3
                  :ptn/plan {:conn  (db-spec)
                             :query "SELECT * FROM_ERR"}}]
      (is (thrown-uncaught?
           PSQLException
           (io/ptn config))))))


(deftest diff-test
  (testing "Without partitioning"
    (let [config  {:match-by [:c1]
                   :ponders  {:c2 3 :c3 2}
                   :workers  2
                   :src/plan {:conn  (db-spec)
                              :query "SELECT * FROM
                                       (VALUES (1, 3, 5, 6),
                                               (2, 7, 9, 8))
                                       AS _ (c1, c2, c3, c4)"}
                   :tgt/plan {:conn  (db-spec)
                              :query "SELECT * FROM
                                       (VALUES (2, 2, 9, 4),
                                               (3, 6, 7, 8))
                                       AS _ (c1, c2, c3, c4)"}}
          ch-ptn  (async/to-chan! [[]])
          ch-diff (io/diff config ch-ptn)]
      (is (= (async/<!! (async/into #{} ch-diff))
             #{{:ins [{:c1 1 :c2 3 :c3 5 :c4 6}]}
               {:del [{:c1 3 :c2 6 :c3 7 :c4 8}]}
               {:upd [{:src  {:c1 2 :c2 7 :c3 9 :c4 8}
                       :tgt  {:c1 2 :c2 2 :c3 9 :c4 4}
                       :cols [:c2]}]}}))))
  (testing "With partitioning"
    (let [config  {:match-by [:c1]
                   :ponders  {:c2 3 :c3 2}
                   :workers  2
                   :src/plan {:conn  (db-spec)
                              :query "SELECT * FROM
                                       (VALUES (1, 3, 5, 6),
                                               (2, 7, 9, 8))
                                       AS _ (c1, c2, c3, c4)
                                      WHERE c1 IN (?, ?, ?)"}
                   :tgt/plan {:conn  (db-spec)
                              :query "SELECT * FROM
                                       (VALUES (2, 2, 9, 4),
                                               (3, 6, 7, 8))
                                       AS _ (c1, c2, c3, c4)
                                      WHERE c1 IN (?, ?, ?)"}}
          ch-ptn  (async/to-chan! [[1 2 3] [4 4 4]])
          ch-diff (io/diff config ch-ptn)]
      (is (= (async/<!! (async/into #{} ch-diff))
             #{{:ins [{:c1 1 :c2 3 :c3 5 :c4 6}]}
               {:del [{:c1 3 :c2 6 :c3 7 :c4 8}]}
               {:upd [{:src  {:c1 2 :c2 7 :c3 9 :c4 8}
                       :tgt  {:c1 2 :c2 2 :c3 9 :c4 4}
                       :cols [:c2]}]}}))))
  (testing "Unrecoverable error handling"
    (try
      (let [config {:match-by [:c1]
                    :ponders  {:c2 3 :c3 2}
                    :workers  1
                    :src/plan {:conn  (assoc (db-spec) :dbtype "oracle")
                               :query "SELECT * FROM
                                        (VALUES (1)) AS _ (c1)
                                       WHERE c1 IN (?)"}
                    :tgt/plan {:conn  (db-spec)
                               :query "SELECT * FROM
                                        (VALUES (2)) AS _ (c1)
                                       WHERE c1 IN (?)"}}
            ch-err  (impl/uncaught-ex-chan)
            ch-ptn  (async/to-chan! [[1]])
            ch-diff (io/diff config ch-ptn)]
        (is (instance? java.sql.SQLException (async/<!! ch-err)))
        (async/close! ch-err)
        (is (drained? ch-err))
        (is (= (async/<!! (async/into [] ch-ptn)) [[1]]))
        (is (drained? ch-diff)))
      (finally (Thread/setDefaultUncaughtExceptionHandler nil))))
  (testing "Recoverable error handling"
    (try
      (let [config  {:match-by [:c1]
                     :ponders  {:c2 3 :c3 2}
                     :workers  2
                     :src/plan {:conn  (db-spec)
                                :query "SELECT * FROM
                                         (VALUES (1)) AS _ (c1)
                                        WHERE c1 IN (?)"}
                     :tgt/plan {:conn (db-spec)
                                :query "SELECT * FROM_ERR
                                         (VALUES (2)) AS _ (c1)
                                        WHERE c1 IN (?)"}}
            ch-err  (impl/uncaught-ex-chan)
            ch-ptn  (async/to-chan! [[1]])
            ch-diff (io/diff config ch-ptn)]
        (let [ex (async/<!! ch-err)]
          (is (instance? PSQLException (ex-cause ex)))
          (is (= (ex-data ex) {:ptn [1]})))
        (async/close! ch-err)
        (is (drained? ch-err))
        (is (drained? ch-diff)))
      (finally (Thread/setDefaultUncaughtExceptionHandler nil)))))


(deftest split-by-diff-test
  (let [ch-diff (async/chan)
        {:keys [ins del upd]}
        (io/split-by-diff 5 ch-diff)
        ch-ins (async/into #{} ins)
        ch-del (async/into #{} del)
        ch-upd (async/into #{} upd)]
    (async/>!! ch-diff {:ins [{:foo 1 :bar 2}]})
    (async/>!! ch-diff {:ins [{:foo 2 :bar 3}
                              {:foo 3 :bar 4}]
                        :del [{:foo 1 :bar 2}
                              {:foo 2 :bar 3}]
                        :upd [{:src  {:foo 1 :bar 2}
                               :tgt  {:foo 2 :bar 2}
                               :cols [:foo]}
                              {:src  {:foo 1 :bar 2}
                               :tgt  {:foo 1 :bar 1}
                               :cols [:bar]}]})
    (async/>!! ch-diff {})
    (async/>!! ch-diff {:foo [{:foo 1 :bar 2}]})
    (async/close! ch-diff)
    (is (= (async/<!! ch-ins)
           #{{:foo 1 :bar 2}
             {:foo 2 :bar 3}
             {:foo 3 :bar 4}}))
    (is (= (async/<!! ch-del)
           #{{:foo 1 :bar 2}
             {:foo 2 :bar 3}}))
    (is (= (async/<!! ch-upd)
           #{{:src  {:foo 1 :bar 2}
              :tgt  {:foo 2 :bar 2}
              :cols [:foo]}
             {:src  {:foo 1 :bar 2}
              :tgt  {:foo 1 :bar 1}
              :cols [:bar]}}))))
