;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.util-test
  (:require
   [clojure.test :refer [deftest testing is are]]
   [diff-rdb.impl.util :as impl]))


(deftest equalize-ptn-test
  (is (= (impl/equalize-ptn 5 [1])
         [1 1 1 1 1]))
  (is (= (impl/equalize-ptn 5 [1 2 3])
         [1 2 3 1 1]))
  (testing "Trash cases"
    (is (= (impl/equalize-ptn -5 [1 2 3])
           (impl/equalize-ptn  0 [1 2 3])
           (impl/equalize-ptn  2 [1 2 3])
           (impl/equalize-ptn  3 [1 2 3])
           [1 2 3]))
    (is (= (impl/equalize-ptn 3 [])
           (impl/equalize-ptn 3 nil)
           [nil nil nil]))
    (is (= (impl/equalize-ptn -5 []) []))
    (is (= (impl/equalize-ptn -5 nil) nil))))


(deftest ptn-xf-test
  (are [size input _ output]
      (= (into [] (impl/ptn-xf size) input)
         output)

    0 nil   :=> []
    3 nil   :=> []
    0 []    :=> []
    3 []    :=> []
    0 [nil] :=> []
    3 [nil] :=> []
    0 [[]]  :=> []
    3 [[]]  :=> []

    0 [[1]]       :=> [[1]]
    0 [[1] [2]]   :=> [[1 2]]
    0 [[1 2 3]]   :=> [[1 2 3]]
    0 [[1 2] [3]] :=> [[1 2 3]]

    1 [[1]]       :=> [[1]]
    1 [[1] [2]]   :=> [[1] [2]]
    1 [[1 2 3]]   :=> [[1] [2] [3]]
    1 [[1 2] [3]] :=> [[1] [2] [3]]

    3 [[1]]                     :=> [[1 1 1]]
    3 [[1] [2]]                 :=> [[1 2 1]]
    3 [[1] [2] [3]]             :=> [[1 2 3]]
    3 [[1] [2] [3] [4]]         :=> [[1 2 3] [4 4 4]]
    3 [[1] [2] [3] [4] [5] [6]] :=> [[1 2 3] [4 5 6]]
    3 [[1 2]]                   :=> [[1 2 1]]
    3 [[1 2 3]]                 :=> [[1 2 3]]
    3 [[1 2] [3]]               :=> [[1 2 3]]
    3 [[1 2 3 4]]               :=> [[1 2 3] [4 4 4]]
    3 [[1 2 3] [4]]             :=> [[1 2 3] [4 4 4]]
    3 [[1 2 3] [4 5] [6]]       :=> [[1 2 3] [4 5 6]]
    3 [[1 2 3 4 5 6]]           :=> [[1 2 3] [4 5 6]])

  (is (thrown? NullPointerException (impl/ptn-xf nil))))


(deftest expand-?s-test
  (is (= (impl/expand-?s 3 "IN(?s)")
         "IN(?,?,?)"))
  (testing "Trash cases"
    (is (thrown?
         NullPointerException
         (impl/expand-?s nil "IN(?s)")))
    (is (= (impl/expand-?s -3 "IN(?s)")
           (impl/expand-?s  0 "IN(?s)")
           (impl/expand-?s  3 "IN()")
           "IN()"))))
