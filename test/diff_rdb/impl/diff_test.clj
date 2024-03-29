;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.diff-test
  (:require
   [clojure.test :refer [deftest is are]]
   [diff-rdb.impl.diff :as impl]
   [clojure.set]
   [clojure.walk]))


(deftest pair-groups-test
  (are [g1 g2 ret]
      (->> (impl/pair-groups g2 g1)
           (map reverse)
           (vector ret (impl/pair-groups g1 g2))
           (map #(sort-by hash %))
           (apply =))

    nil nil #_=> []
    {}  nil #_=> []
    {}  {}  #_=> []

    {:foo nil} nil        #_=> [[nil nil]]
    {:foo nil} {}         #_=> [[nil nil]]
    {:foo nil} {:foo nil} #_=> [[nil nil]]

    {:foo 5}
    {:foo 6}
    #_=> [[5 6]]

    {:foo 5}
    {:bar 5}
    #_=> [[5 nil]
          [nil 5]]

    {:foo [1] :bar [2]}
    {:foo [3] :bar [4]}
    #_=> [[[1] [3]]
          [[2] [4]]]))


(deftest distinct-by-test
  (are [arg ret]
      (= (impl/distinct-by first second arg)
         (impl/distinct-by second first arg)
         ret)

    nil #_=> []
    []  #_=> []

    [[1 2 3]] #_=> [[1 2 3]]

    [[0 0] [0 1] [0 2] [0 3]
     [1 0] [1 1] [1 2] [1 3]]
    #_=> [[0 0] [1 1]]

    [[0 0 0 0] [0 1 1 0] [0 2 2 0] [0 3 3 0]
     [1 0 0 1] [1 1 1 1] [1 2 2 1] [1 3 3 1]]
    #_=> [[0 0 0 0] [1 1 1 1]]

    [[0] [1] [2] [3]] #_=> [[0]])

  (is (= (impl/distinct-by nil nil nil)
         (impl/distinct-by nil nil [])
         []))
  (is (thrown?
       NullPointerException
       (impl/distinct-by nil nil [1 1]))))


(deftest diff-rows-test
  (are [compare-cols ponders src tgt ret]
      (->> (impl/diff-rows compare-cols ponders tgt src)
           (clojure.walk/postwalk
            #(if (map? %)
               (clojure.set/rename-keys
                % {:ins :del, :del :ins
                   :src :tgt, :tgt :src}) %))
           (= ret (impl/diff-rows compare-cols ponders src tgt)))

    ;; NONE TO NONE

    nil nil nil nil #_=> {}
    []  nil nil nil #_=> {}
    nil {}  nil nil #_=> {}
    nil nil []  nil #_=> {}
    []  {}  nil nil #_=> {}
    nil {}  []  nil #_=> {}
    []  {}  []  []  #_=> {}

    [:foo] {:foo 5} nil nil #_=> {}
    [:foo] {:foo 5} []  []  #_=> {}
    [:foo] {:foo 5} nil []  #_=> {}

    ;; SEQ TO NONE

    [:foo]
    {:foo 5}
    [{:foo 1 :bar 2}]
    nil
    #_=> {:ins [{:foo 1 :bar 2}]}

    [:foo]
    {:foo 5}
    [{:foo 1 :bar 2}]
    []
    #_=> {:ins [{:foo 1 :bar 2}]}

    [:foo]
    {:foo 5}
    [{:foo 1 :bar 2}
     {:foo 2 :bar 3}]
    nil
    #_=> {:ins [{:foo 1 :bar 2}
                {:foo 2 :bar 3}]}

    [:foo]
    {:foo 5}
    [{:foo 1 :bar 2}
     {:foo 2 :bar 3}]
    []
    #_=> {:ins [{:foo 1 :bar 2}
                {:foo 2 :bar 3}]}

    ;; ONE TO ONE

    [:foo :bar :baz]
    {:foo 1 :bar 2 :baz 3}
    [{:id  1 :foo 1 :bar 2 :baz 3}]
    [{:id 11 :foo 2 :bar 2 :baz 4}]
    #_=> {:upd [{:src  {:id  1 :foo 1 :bar 2 :baz 3}
                 :tgt  {:id 11 :foo 2 :bar 2 :baz 4}
                 :cols [:foo :baz]}]}

    ;; All rows are considered eq when
    ;; no compare-cols are provided
    []
    {}
    [{:foo 1 :bar 2}]
    [{:foo 3 :bar 5}]
    #_=> {}

    ;; ONE TO MANY

    [:foo :bar]
    {:foo 1 :bar 2}
    [{:id  1 :foo 1 :bar 2}]
    [{:id 11 :foo 1 :bar 4}
     {:id 22 :foo 2 :bar 2}]
    #_=> {:del [{:id 11 :foo 1 :bar 4}]
          :upd [{:src  {:id  1 :foo 1 :bar 2}
                 :tgt  {:id 22 :foo 2 :bar 2}
                 :cols [:foo]}]}

    ;; MANY TO MANY

    ;; All eq rows
    [:bar :baz]
    {:bar 1 :baz 2}
    [{:id  1 :foo 1 :bar 1 :baz 1}
     {:id  2 :foo 2 :bar 2 :baz 2}]
    [{:id 11 :foo 1 :bar 1 :baz 1}
     {:id 22 :foo 2 :bar 2 :baz 2}]
    #_=> {}

    ;; Some eq rows
    [:bar :baz]
    {:bar 1 :baz 2}
    [{:id  1 :foo 1 :bar 1 :baz 1}
     {:id  2 :foo 2 :bar 2 :baz 2}]
    [{:id 11 :foo 1 :bar 1 :baz 3}
     {:id 22 :foo 2 :bar 2 :baz 2}]
    #_=> {:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                 :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                 :cols [:baz]}]}

    ;; Same count
    [:foo :bar :baz]
    {:foo 1 :bar 2 :baz 3}
    [{:id  1 :foo 1 :bar 2 :baz 4}
     {:id  2 :foo 2 :bar 4 :baz 3}]
    [{:id 11 :foo 3 :bar 2 :baz 3}
     {:id 22 :foo 4 :bar 2 :baz 4}]
    #_=> {:upd [{:src  {:id  1 :foo 1 :bar 2 :baz 4}
                 :tgt  {:id 22 :foo 4 :bar 2 :baz 4}
                 :cols [:foo]}
                {:src  {:id  2 :foo 2 :bar 4 :baz 3}
                 :tgt  {:id 11 :foo 3 :bar 2 :baz 3}
                 :cols [:foo :bar]}]}

    ;; Different count
    [:foo :bar :baz]
    {:foo 1 :bar 2 :baz 3}
    [{:id  1 :foo 1 :bar 2 :baz 4}
     {:id  2 :foo 2 :bar 4 :baz 3}
     {:id  3 :foo 3 :bar 6 :baz 7}
     {:id  4 :foo 4 :bar 7 :baz 6}]
    [{:id 11 :foo 5 :bar 2 :baz 3}
     {:id 22 :foo 6 :bar 2 :baz 4}]
    #_=> {:ins [{:id 3 :foo 3 :bar 6 :baz 7}
                {:id 4 :foo 4 :bar 7 :baz 6}]
          :upd [{:src  {:id  1 :foo 1 :bar 2 :baz 4}
                 :tgt  {:id 22 :foo 6 :bar 2 :baz 4}
                 :cols [:foo]}
                {:src  {:id  2 :foo 2 :bar 4 :baz 3}
                 :tgt  {:id 11 :foo 5 :bar 2 :baz 3}
                 :cols [:foo :bar]}]}

    ;; Same ponder sum
    [:foo :bar :baz]
    {:foo 1/2 :bar 1/2 :baz 1}
    [{:id  1 :foo 1 :bar 1 :baz 3}
     {:id  2 :foo 2 :bar 2 :baz 3}]
    [{:id 11 :foo 1 :bar 1 :baz 4}
     {:id 22 :foo 3 :bar 3 :baz 3}]
    #_=> {:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 3}
                 :tgt  {:id 11 :foo 1 :bar 1 :baz 4}
                 :cols [:baz]}
                {:src  {:id  2 :foo 2 :bar 2 :baz 3}
                 :tgt  {:id 22 :foo 3 :bar 3 :baz 3}
                 :cols [:foo :bar]}]}

    ;; Duplicate rows
    [:foo :bar :baz]
    {:foo 0.5 :bar 0.5 :baz 1}
    [{:id  1 :foo 1 :bar 1 :baz 3}
     {:id  2 :foo 2 :bar 2 :baz 3}
     {:id  2 :foo 2 :bar 2 :baz 3}
     {:id  2 :foo 2 :bar 2 :baz 3}]
    [{:id 11 :foo 1 :bar 1 :baz 4}
     {:id 22 :foo 3 :bar 3 :baz 3}
     {:id 22 :foo 3 :bar 3 :baz 3}]
    #_=> {:ins [{:id 2 :foo 2 :bar 2 :baz 3}]
          :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 3}
                 :tgt  {:id 11 :foo 1 :bar 1 :baz 4}
                 :cols [:baz]}
                {:src  {:id  2 :foo 2 :bar 2 :baz 3}
                 :tgt  {:id 22 :foo 3 :bar 3 :baz 3}
                 :cols [:foo :bar]}
                {:src  {:id  2 :foo 2 :bar 2 :baz 3}
                 :tgt  {:id 22 :foo 3 :bar 3 :baz 3}
                 :cols [:foo :bar]}]}

    ;; TRASH CASES

    ;; Expects (= compare-cols (keys ponders))

    []
    {:foo 1 :bar 2}
    [{:foo 1 :bar 2}
     {:foo 1 :bar 2}]
    [{:foo 3 :bar 5}]
    #_=> {:ins [{:foo 1 :bar 2}]})

  (is (thrown?
       NullPointerException
       (impl/diff-rows
        [:foo :bar]
        {}
        [{:foo 1 :bar 2}
         {:foo 1 :bar 2}]
        [{:foo 3 :bar 5}])))
  (is (thrown?
       NullPointerException
       (impl/diff-rows
        [:foo :bar]
        {:foo 1}
        [{:foo 1 :bar 2}
         {:foo 1 :bar 2}]
        [{:foo 3 :bar 5}]))))
