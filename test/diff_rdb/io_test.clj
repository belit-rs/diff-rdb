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
