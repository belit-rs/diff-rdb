;; Copyright (c) Belit d.o.o.


(ns diff-rdb.diff-test
  (:require
   [clojure.test :refer [deftest testing is are]]
   [clojure.spec.test.alpha :as st]
   [diff-rdb.diff :refer [diff]]
   [clojure.set]
   [clojure.walk])
  (:import
   (clojure.lang ArityException)
   (clojure.lang PersistentVector)))


(defn sort-vectors
  [m]
  (clojure.walk/postwalk
   #(if (instance? PersistentVector %)
      (vec (sort-by hash %))
      %)
   m))


(defn test-diff
  [config src tgt ret]
  (->> (diff config tgt src)
       (clojure.walk/postwalk
        #(if (map? %)
           (clojure.set/rename-keys
            % {:ins :del, :del :ins
               :src :tgt, :tgt :src}) %))
       (vector ret (diff config src tgt))
       (map sort-vectors)
       (apply =)))


(deftest diff-test
  (testing "Standard cases"
    (are [config src tgt ret]
        (test-diff config src tgt ret)

      ;; NONE TO NONE

      {:match-by [:foo]}                   [] [] #_=> {}
      {:match-by [:foo] :ponders {:bar 1}} [] [] #_=> {}

      ;; SEQ TO NONE

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      []
      #_=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      []
      #_=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:foo]}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      []
      #_=> {:ins [{:foo 1 :bar 1}
                  {:foo 2 :bar 2}]}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      []
      #_=> {:ins [{:foo 1 :bar 1}
                  {:foo 2 :bar 2}]}

      ;; ONE TO ONE

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 1}]
      #_=> {}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 1}]
      #_=> {}

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 2}]
      #_=> {}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 2}]
      #_=> {:upd [{:src  {:foo 1 :bar 1}
                   :tgt  {:foo 1 :bar 2}
                   :cols [:bar]}]}

      {:match-by [:foo :bar]}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 2}]
      #_=> {:ins [{:foo 1 :bar 1}]
            :del [{:foo 1 :bar 2}]}

      ;; ONE TO MANY

      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 1 :baz 2}]
      #_=> {:del [{:id 11 :foo 1 :bar 3 :baz 3}]
            :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 22 :foo 1 :bar 1 :baz 2}
                   :cols [:baz]}]}

      ;; Same ponder sum
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 4 :baz 2}]
      #_=> {:del [{:id 22 :foo 1 :bar 4 :baz 2}]
            :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 11 :foo 1 :bar 3 :baz 3}
                   :cols [:bar :baz]}]}

      {:match-by [:foo]}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 1 :baz 2}]
      #_=> {:del [{:id 22 :foo 1 :bar 1 :baz 2}]}

      {:match-by [:foo :bar]}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 1 :baz 2}]
      #_=> {:del [{:id 11 :foo 1 :bar 3 :baz 3}]}

      ;; MANY TO MANY

      ;; All eq rows
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 2 :baz 2}]
      [{:id 11 :foo 1 :bar 1 :baz 1}
       {:id 22 :foo 2 :bar 2 :baz 2}]
      #_=> {}

      ;; Some eq rows
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 2 :baz 2}]
      [{:id 11 :foo 1 :bar 1 :baz 3}
       {:id 22 :foo 2 :bar 2 :baz 2}]
      #_=> {:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                   :cols [:baz]}]}

      ;; No eq rows
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 2 :baz 2}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 2 :bar 4 :baz 2}]
      #_=> {:upd [{:src  {:id  2 :foo 2 :bar 2 :baz 2}
                   :tgt  {:id 22 :foo 2 :bar 4 :baz 2}
                   :cols [:bar]}
                  {:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 11 :foo 1 :bar 3 :baz 3}
                   :cols [:bar :baz]}]}

      ;; Different count
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 2 :baz 2}
       {:id  3 :foo 2 :bar 4 :baz 2}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 2 :bar 4 :baz 2}]
      #_=> {:ins [{:id 2 :foo 2 :bar 2 :baz 2}]
            :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 11 :foo 1 :bar 3 :baz 3}
                   :cols [:bar :baz]}]}

      ;; Multiple match-by cols
      {:match-by [:foo :bar]
       :ponders  {:baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 1 :baz 2}
       {:id  3 :foo 2 :bar 4 :baz 2}]
      [{:id 11 :foo 1 :bar 1 :baz 3}
       {:id 22 :foo 2 :bar 4 :baz 2}]
      #_=> {:ins [{:id 2 :foo 2 :bar 1 :baz 2}]
            :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                   :cols [:baz]}]}

      ;; Duplicate rows
      {:match-by [:foo :bar]
       :ponders  {:baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 1 :baz 2}
       {:id  2 :foo 2 :bar 1 :baz 2}
       {:id  2 :foo 2 :bar 1 :baz 2}]
      [{:id 11 :foo 1 :bar 1 :baz 3}
       {:id 22 :foo 2 :bar 4 :baz 2}
       {:id 33 :foo 2 :bar 1 :baz 2}]
      #_=> {:ins [{:id  2 :foo 2 :bar 1 :baz 2}
                  {:id  2 :foo 2 :bar 1 :baz 2}]
            :del [{:id 22 :foo 2 :bar 4 :baz 2}]
            :upd [{:src {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt {:id 11 :foo 1 :bar 1 :baz 3}
                   :cols [:baz]}]}))

  (testing "Trash cases"
    (are [config src tgt ret]
        (st/with-instrument-disabled
          (test-diff config src tgt ret))

      ;; NONE TO NONE

      {:match-by [:foo]}                   nil nil #_=> {}
      {:match-by [:foo]}                   []  nil #_=> {}
      {:match-by [:foo] :ponders {:bar 1}} nil nil #_=> {}
      {:match-by [:foo] :ponders {:bar 1}} []  nil #_=> {}

      ;; SEQ TO NONE

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      nil
      #_=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      nil
      #_=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:bar]}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      nil
      #_=> {:ins [{:foo 1 :bar 1}
                  {:foo 2 :bar 2}]}

      {:match-by [:bar]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      nil
      #_=> {:ins [{:foo 1 :bar 1}
                  {:foo 2 :bar 2}]}

      ;; MANY TO SEQ

      ;; Overlap between match-by and ponders
      {:match-by [:foo :bar]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}]
      #_=> {:ins [{:foo 1 :bar 1}
                  {:foo 1 :bar 4}]
            :del [{:foo 1 :bar 2}]}

      ;; Match-by cols missing in rows
      {:match-by [:baz]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}]
      #_=> {:ins [{:foo 1 :bar 4}]
            :upd [{:src  {:foo 1 :bar 1}
                   :tgt  {:foo 1 :bar 2}
                   :cols [:bar]}]}

      ;; Ponder cols missing in all rows
      {:match-by [:foo]
       :ponders  {:baz 1}}
      [{:foo 1 :bar 1}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}]
      #_=> {:ins [{:foo 1 :bar 4}]}

      ;; Ponder cols missing in some rows
      {:match-by [:foo]
       :ponders  {:baz 1}}
      [{:foo 1 :bar 1 :baz 4}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}
       {:foo 1 :bar 7}]
      #_=> {:upd [{:src  {:foo 1 :bar 1 :baz 4}
                   :tgt  {:foo 1 :bar 7}
                   :cols [:baz]}]})

    ;; Match-by is REQUIRED

    (st/with-instrument-disabled
      (is (thrown? ArityException (diff nil)))
      (is (thrown? ArityException (diff {})))
      (is (thrown? ArityException (diff {:match-by []})))))

  (testing "Transducer"
    (are [xf src-tgt ret]
        (= (sort-by hash ret)
           (sort-by hash (into [] xf src-tgt)))

      (diff {:match-by [:foo]
             :ponders  {:bar 1 :baz 2}})
      [[[{:id  1 :foo 1 :bar 1 :baz 1}]
        [{:id 11 :foo 1 :bar 1 :baz 1}]]]
      #_=> []

      (diff {:match-by [:foo]
             :ponders  {:bar 1 :baz 2}})
      [[[{:id  1 :foo 1 :bar 1 :baz 1}
         {:id  2 :foo 2 :bar 2 :baz 2}]
        [{:id 11 :foo 1 :bar 1 :baz 3}
         {:id 22 :foo 2 :bar 2 :baz 2}]]]
      #_=> [{:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                    :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                    :cols [:baz]}]}]

      (diff {:match-by [:foo :bar]
             :ponders  {:baz 2}})
      [[[{:id  1 :foo 1 :bar 1 :baz 1}
         {:id  2 :foo 2 :bar 1 :baz 2}
         {:id  3 :foo 2 :bar 4 :baz 2}]
        [{:id 11 :foo 1 :bar 1 :baz 3}
         {:id 22 :foo 2 :bar 4 :baz 2}]]]
      #_=> [{:ins [{:id 2 :foo 2 :bar 1 :baz 2}]}
            {:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                    :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                    :cols [:baz]}]}]

      (diff {:match-by [:foo :bar]
             :ponders  {:baz 2}})
      [[[{:id  1 :foo 1 :bar 1 :baz 1}
         {:id  2 :foo 1 :bar 1 :baz 2}
         {:id  3 :foo 1 :bar 1 :baz 2}]
        [{:id 11 :foo 1 :bar 1 :baz 3}
         {:id 22 :foo 2 :bar 4 :baz 2}
         {:id 33 :foo 2 :bar 4 :baz 2}
         {:id 44 :foo 3 :bar 4 :baz 2}]]]
      #_=> [{:ins [{:id 2 :foo 1 :bar 1 :baz 2}
                   {:id 3 :foo 1 :bar 1 :baz 2}]
             :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                    :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                    :cols [:baz]}]}
            {:del [{:id 22 :foo 2 :bar 4 :baz 2}
                   {:id 33 :foo 2 :bar 4 :baz 2}]}
            {:del [{:id 44 :foo 3 :bar 4 :baz 2}]}])))
