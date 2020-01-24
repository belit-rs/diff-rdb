;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.diff-test
  (:require
   [clojure.test :refer :all]
   [diff-rdb.impl.diff :as impl]))


(deftest pair-groups-test
  (are [g1 g2 _ ret]
      (->> (impl/pair-groups g2 g1)
           (map (fn [[k v]] [k (reverse v)]))
           (into {})
           (= ret (impl/pair-groups g1 g2)))

    nil nil :=> {}
    {}  nil :=> {}
    {}  {}  :=> {}

    {:foo nil} nil        :=> {:foo [nil nil]}
    {:foo nil} {}         :=> {:foo [nil nil]}
    {:foo nil} {:foo nil} :=> {:foo [nil nil]}

    {:foo 5}
    {:foo 6}
    :=> {:foo [5 6]}

    {:foo 5}
    {:bar 5}
    :=> {:foo [5 nil]
         :bar [nil 5]}

    {:foo [1] :bar [2]}
    {:foo [3] :bar [4]}
    :=> {:foo [[1] [3]]
         :bar [[2] [4]]}))


(deftest distinct-by-test
  (are [arg _ ret]
      (= (impl/distinct-by first second arg)
         (impl/distinct-by second first arg)
         ret)

    nil :=> []
    []  :=> []

    [[1 2 3]] :=> [[1 2 3]]

    [[0 0] [0 1] [0 2] [0 3]
     [1 0] [1 1] [1 2] [1 3]]
    :=> [[0 0] [1 1]]

    [[0 0 0 0] [0 1 1 0] [0 2 2 0] [0 3 3 0]
     [1 0 0 1] [1 1 1 1] [1 2 2 1] [1 3 3 1]]
    :=> [[0 0 0 0] [1 1 1 1]]

    [[0] [1] [2] [3]] :=> [[0]])

  (is (= (impl/distinct-by nil nil nil)
         (impl/distinct-by nil nil [])
         []))
  (is (thrown?
       NullPointerException
       (impl/distinct-by nil nil [1 1]))))


(deftest remove-eq-rows-test
  (are [compare-cols src-tgt _ ret]
      (->> (reverse src-tgt)
           (impl/remove-eq-rows compare-cols) reverse
           (= ret (impl/remove-eq-rows compare-cols src-tgt)))

    ;; NONE TO NONE

    [:foo] [nil nil] :=> [nil nil]
    [:foo] [nil [] ] :=> [nil nil]
    [:foo] [[]  [] ] :=> [nil nil]

    ;; SEQ TO NONE

    [:foo] [[{:foo 2}] nil]
    :=>    [[{:foo 2}] nil]

    [:foo] [[{:foo 2} {:foo 3}] nil]
    :=>    [[{:foo 2} {:foo 3}] nil]

    [:foo] [[{:foo 2}] []]
    :=>    [[{:foo 2}] nil]

    [:foo] [[{:foo 2} {:foo 3}] []]
    :=>    [[{:foo 2} {:foo 3}] nil]

    ;; ONE TO ONE

    [:foo]
    [[{:foo 2}]
     [{:foo 2}]]
    :=> [nil nil]

    [:foo]
    [[{:foo 2}]
     [{:foo 3}]]
    :=> [[{:foo 2}] [{:foo 3}]]

    ;; ONE TO MANY

    [:foo]
    [[{:foo 2}]
     [{:foo 2} {:foo 3}]]
    :=> [nil [{:foo 3}]]

    [:foo]
    [[{:foo 1}]
     [{:foo 2} {:foo 3}]]
    :=> [[{:foo 1}]
         [{:foo 2} {:foo 3}]]

    ;; MANY TO MANY

    ;; All eq rows
    [:foo :bar]
    [[{:id  1 :foo 1 :bar 1}
      {:id  2 :foo 2 :bar 2}]
     [{:id 11 :foo 1 :bar 1}
      {:id 22 :foo 2 :bar 2}]]
    :=> [nil nil]

    ;; Some eq rows
    [:foo :bar]
    [[{:id  1 :foo 1 :bar 1}
      {:id  2 :foo 2 :bar 2}
      {:id  3 :foo 2 :bar 2}]
     [{:id 11 :foo 1 :bar 1}
      {:id 22 :foo 2 :bar 2}]]
    :=> [[{:id 3 :foo 2 :bar 2}]
         nil]

    ;; Some eq rows
    [:foo :bar]
    [[{:id  1 :foo 1}
      {:id  2 :foo 2 :bar 2}]
     [{:id 11 :foo 1 :bar nil}
      {:id 22 :foo 2}]]
    :=> [[{:id  2 :foo 2 :bar 2}]
         [{:id 22 :foo 2}]]

    ;; No eq rows
    [:foo :bar]
    [[{:id  1 :foo 1 :bar 1}
      {:id  2 :foo 2 :bar 2}
      {:id  3 :foo 3 :bar 3}]
     [{:id 11 :foo 4 :bar 4}
      {:id 22 :foo 5 :bar 5}]]
    :=> [[{:id  1 :foo 1 :bar 1}
          {:id  2 :foo 2 :bar 2}
          {:id  3 :foo 3 :bar 3}]
         [{:id 11 :foo 4 :bar 4}
          {:id 22 :foo 5 :bar 5}]]

    ;; Duplicate rows
    [:foo :bar]
    [[{:id  1 :foo 1 :bar 1}
      {:id  2 :foo 2 :bar 2}]
     [{:id 11 :foo 1 :bar 1}
      {:id 22 :foo 2 :bar 2}
      {:id 22 :foo 2 :bar 2}]]
    :=> [nil
         [{:id 22 :foo 2 :bar 2}]]

    ;; TRASH CASES

    nil    nil                     :=> [nil nil]
    nil    []                      :=> [nil nil]
    []     []                      :=> [nil nil]
    [:foo] nil                     :=> [nil nil]
    nil    [[{:bar 2}] [{:bar 3}]] :=> [nil nil]
    []     [[{:bar 2}] [{:bar 3}]] :=> [nil nil]
    [:foo] [[{:bar 2}] [{:bar 3}]] :=> [nil nil]
    [nil]  [[{:bar 2}] [{:bar 3}]] :=> [nil nil]))


(deftest diff-cols-test
  (are [compare-cols m1 m2 _ ret]
      (= (impl/diff-cols compare-cols m1 m2)
         (impl/diff-cols compare-cols m2 m1)
         ret)

    [:foo :bar :baz]
    {:id 1 :foo "1" :bar 2 :baz 3}
    {:id 2 :foo "3" :bar 2 :baz 1}
    :=> [:foo :baz]

    [:foo :bar]
    {:foo :x :bar nil :baz 4}
    {:foo :x :bar   3 :baz 3}
    :=> [:bar]

    [:foo :bar]
    {:foo :x :baz 4}
    {:foo :x :baz 3 :bar 3}
    :=> [:bar]

    [:foo :bar] {:foo 1 :bar 2} {:foo 1 :bar 2} :=> []
    [:foo]      {:foo nil}      {:foo nil}      :=> []
    [:foo]      {:foo nil}      {}              :=> []
    [:foo]      {:foo nil}      nil             :=> []

    nil    nil      nil      :=> []
    [nil]  nil      nil      :=> []
    [:foo] nil      nil      :=> []
    [:foo] {}       {}       :=> []
    [:foo] nil      {}       :=> []
    [:foo] {}       nil      :=> []
    nil    {:foo 5} {:foo 4} :=> []
    []     {:foo 5} {:foo 4} :=> []))


(deftest diff-rows-test
  (are [compare-cols ponders src-tgt _ ret]
      (->> (reverse src-tgt)
           (impl/diff-rows compare-cols ponders)
           (clojure.walk/postwalk
            #(if (map? %)
               (clojure.set/rename-keys
                % {:ins :del, :del :ins
                   :src :tgt, :tgt :src}) %))
           (= ret (impl/diff-rows compare-cols ponders src-tgt)))

    ;; SEQ TO NONE

    [:foo]
    {:foo 5}
    [[{:foo 1 :bar 2}]
     nil]
    :=> {:ins [{:foo 1 :bar 2}]}

    [:foo]
    {:foo 5}
    [[{:foo 1 :bar 2}]
     []]
    :=> {:ins [{:foo 1 :bar 2}]}

    [:foo]
    {:foo 5}
    [[{:foo 1 :bar 2}
      {:foo 2 :bar 3}]
     nil]
    :=> {:ins [{:foo 1 :bar 2}
               {:foo 2 :bar 3}]}

    [:foo]
    {:foo 5}
    [[{:foo 1 :bar 2}
      {:foo 2 :bar 3}]
     []]
    :=> {:ins [{:foo 1 :bar 2}
               {:foo 2 :bar 3}]}

    ;; ONE TO ONE

    [:foo :bar :baz]
    {:foo 1 :bar 2 :baz 3}
    [[{:id  1 :foo 1 :bar 2 :baz 3}]
     [{:id 11 :foo 2 :bar 2 :baz 4}]]
    :=> {:upd [{:src  {:id  1 :foo 1 :bar 2 :baz 3}
                :tgt  {:id 11 :foo 2 :bar 2 :baz 4}
                :cols [:foo :baz]}]}

    ;; ONE TO MANY

    [:foo :bar]
    {:foo 1 :bar 2}
    [[{:id  1 :foo 1 :bar 2}]
     [{:id 11 :foo 1 :bar 4}
      {:id 22 :foo 2 :bar 2}]]
    :=> {:del [{:id 11 :foo 1 :bar 4}]
         :upd [{:src  {:id  1 :foo 1 :bar 2}
                :tgt  {:id 22 :foo 2 :bar 2}
                :cols [:foo]}]}

    ;; MANY TO MANY

    ;; Same count
    [:foo :bar :baz]
    {:foo 1 :bar 2 :baz 3}
    [[{:id  1 :foo 1 :bar 2 :baz 4}
      {:id  2 :foo 2 :bar 4 :baz 3}]
     [{:id 11 :foo 3 :bar 2 :baz 3}
      {:id 22 :foo 4 :bar 2 :baz 4}]]
    :=> {:upd [{:src  {:id  1 :foo 1 :bar 2 :baz 4}
                :tgt  {:id 22 :foo 4 :bar 2 :baz 4}
                :cols [:foo]}
               {:src  {:id  2 :foo 2 :bar 4 :baz 3}
                :tgt  {:id 11 :foo 3 :bar 2 :baz 3}
                :cols [:foo :bar]}]}

    ;; Different count
    [:foo :bar :baz]
    {:foo 1 :bar 2 :baz 3}
    [[{:id  1 :foo 1 :bar 2 :baz 4}
      {:id  2 :foo 2 :bar 4 :baz 3}
      {:id  3 :foo 3 :bar 6 :baz 7}
      {:id  4 :foo 4 :bar 7 :baz 6}]
     [{:id 11 :foo 5 :bar 2 :baz 3}
      {:id 22 :foo 6 :bar 2 :baz 4}]]
    :=> {:ins [{:id 3 :foo 3 :bar 6 :baz 7}
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
    [[{:id  1 :foo 1 :bar 1 :baz 3}
      {:id  2 :foo 2 :bar 2 :baz 3}]
     [{:id 11 :foo 1 :bar 1 :baz 4}
      {:id 22 :foo 3 :bar 3 :baz 3}]]
    :=> {:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 3}
                :tgt  {:id 11 :foo 1 :bar 1 :baz 4}
                :cols [:baz]}
               {:src  {:id  2 :foo 2 :bar 2 :baz 3}
                :tgt  {:id 22 :foo 3 :bar 3 :baz 3}
                :cols [:foo :bar]}]}

    ;; Duplicate rows
    [:foo :bar :baz]
    {:foo 0.5 :bar 0.5 :baz 1}
    [[{:id  1 :foo 1 :bar 1 :baz 3}
      {:id  2 :foo 2 :bar 2 :baz 3}
      {:id  2 :foo 2 :bar 2 :baz 3}
      {:id  2 :foo 2 :bar 2 :baz 3}]
     [{:id 11 :foo 1 :bar 1 :baz 4}
      {:id 22 :foo 3 :bar 3 :baz 3}
      {:id 22 :foo 3 :bar 3 :baz 3}]]
    :=> {:ins [{:id 2 :foo 2 :bar 2 :baz 3}]
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

    nil nil nil :=> {}
    []  nil nil :=> {}
    nil {}  nil :=> {}
    nil nil []  :=> {}
    []  {}  nil :=> {}
    nil {}  []  :=> {}
    []  {}  []  :=> {}

    ;; Expects seq src or seq tgt
    [:foo] {:foo 5} [       ] :=> {}
    [:foo] {:foo 5} [nil nil] :=> {}
    [:foo] {:foo 5} [[]  [] ] :=> {}
    [:foo] {:foo 5} [nil [] ] :=> {}

    ;; Expects no equal (by compare-cols) rows
    [:foo]
    {:foo 5}
    [[{:foo 1 :bar 2}]
     [{:foo 1 :bar 2}]]
    :=> {:upd [{:src  {:foo 1 :bar 2}
                :tgt  {:foo 1 :bar 2}
                :cols []}]}

    ;; Compare cols and ponders are required unless
    ;; match-by doesn't produce some? in src and tgt
    []
    {}
    [[{:foo 1 :bar 2}]
     [{:foo 3 :bar 5}]]
    :=> {:upd [{:src {:foo 1 :bar 2}
                :tgt {:foo 3 :bar 5}
                :cols []}]}

    []
    {}
    [[{:foo 1 :bar 2}]
     []]
    :=> {:ins [{:foo 1 :bar 2}]}

    ;; Expects (= compare-cols (keys ponders))
    []
    {:foo 1 :bar 2}
    [[{:foo 1 :bar 2}
      {:foo 1 :bar 2}]
     [{:foo 3 :bar 5}]]
    :=> {:ins [{:foo 1 :bar 2}]
         :upd [{:src {:foo 1 :bar 2}
                :tgt {:foo 3 :bar 5}
                :cols []}]})

  ;; Expects (= compare-cols (keys ponders))
  (is (thrown?
       NullPointerException
       (impl/diff-rows
        [:foo :bar]
        {}
        [[{:foo 1 :bar 2}
          {:foo 1 :bar 2}]
         [{:foo 3 :bar 5}]])))
  (is (thrown?
       NullPointerException
       (impl/diff-rows
        [:foo :bar]
        {:foo 1}
        [[{:foo 1 :bar 2}
          {:foo 1 :bar 2}]
         [{:foo 3 :bar 5}]]))))
