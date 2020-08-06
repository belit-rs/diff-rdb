;; Copyright (c) Belit d.o.o.


(ns diff-rdb.diff-test
  (:require
   [clojure.test :refer :all]
   [clojure.spec.test.alpha :as st]
   [diff-rdb.diff :refer [diff]])
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
    (are [config src tgt _ ret]
        (test-diff config src tgt ret)

      ;; NONE TO NONE

      {:match-by [:foo]}                   [] [] :=> {}
      {:match-by [:foo] :ponders {:bar 1}} [] [] :=> {}

      ;; SEQ TO NONE

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      []
      :=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      []
      :=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:foo]}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      []
      :=> {:ins [{:foo 1 :bar 1}
                 {:foo 2 :bar 2}]}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      []
      :=> {:ins [{:foo 1 :bar 1}
                 {:foo 2 :bar 2}]}

      ;; ONE TO ONE

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 1}]
      :=> {}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 1}]
      :=> {}

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 2}]
      :=> {}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 2}]
      :=> {:upd [{:src  {:foo 1 :bar 1}
                  :tgt  {:foo 1 :bar 2}
                  :cols [:bar]}]}

      {:match-by [:foo :bar]}
      [{:foo 1 :bar 1}]
      [{:foo 1 :bar 2}]
      :=> {:ins [{:foo 1 :bar 1}]
           :del [{:foo 1 :bar 2}]}

      ;; ONE TO MANY

      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 1 :baz 2}]
      :=> {:del [{:id 11 :foo 1 :bar 3 :baz 3}]
           :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                  :tgt  {:id 22 :foo 1 :bar 1 :baz 2}
                  :cols [:baz]}]}

      ;; Same ponder sum
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 4 :baz 2}]
      :=> {:del [{:id 22 :foo 1 :bar 4 :baz 2}]
           :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                  :tgt  {:id 11 :foo 1 :bar 3 :baz 3}
                  :cols [:bar :baz]}]}

      {:match-by [:foo]}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 1 :baz 2}]
      :=> {:del [{:id 22 :foo 1 :bar 1 :baz 2}]}

      {:match-by [:foo :bar]}
      [{:id  1 :foo 1 :bar 1 :baz 1}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 1 :bar 1 :baz 2}]
      :=> {:del [{:id 11 :foo 1 :bar 3 :baz 3}]}

      ;; MANY TO MANY

      ;; All eq rows
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 2 :baz 2}]
      [{:id 11 :foo 1 :bar 1 :baz 1}
       {:id 22 :foo 2 :bar 2 :baz 2}]
      :=> {}

      ;; Some eq rows
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 2 :baz 2}]
      [{:id 11 :foo 1 :bar 1 :baz 3}
       {:id 22 :foo 2 :bar 2 :baz 2}]
      :=> {:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                  :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                  :cols [:baz]}]}

      ;; No eq rows
      {:match-by [:foo]
       :ponders  {:bar 1 :baz 2}}
      [{:id  1 :foo 1 :bar 1 :baz 1}
       {:id  2 :foo 2 :bar 2 :baz 2}]
      [{:id 11 :foo 1 :bar 3 :baz 3}
       {:id 22 :foo 2 :bar 4 :baz 2}]
      :=> {:upd [{:src  {:id  2 :foo 2 :bar 2 :baz 2}
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
      :=> {:ins [{:id 2 :foo 2 :bar 2 :baz 2}]
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
      :=> {:ins [{:id 2 :foo 2 :bar 1 :baz 2}]
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
      :=> {:ins [{:id  2 :foo 2 :bar 1 :baz 2}
                 {:id  2 :foo 2 :bar 1 :baz 2}]
           :del [{:id 22 :foo 2 :bar 4 :baz 2}]
           :upd [{:src {:id  1 :foo 1 :bar 1 :baz 1}
                  :tgt {:id 11 :foo 1 :bar 1 :baz 3}
                  :cols [:baz]}]}

      ;; Example from doc/api.md
      {:match-by [:name :edition]
       :ponders  {:pages     1
                  :published 2}}
      [{:id 11 :name "Effective Java"                 :pages 414 :edition 3 :published "2018-01-06"}
       {:id 12 :name "Java Concurrency in Practice"   :pages 412 :edition 1 :published "2006-05-19"}
       {:id 13 :name "The Joy of Clojure"             :pages 520 :edition 2 :published "2014-06-13"}
       {:id 14 :name "Clojure Programming"            :pages 630 :edition 1 :published "2012-04-22"}
       {:id 15 :name "Concurrent Programming in Java" :pages 422 :edition 2 :published "1999-11-04"}
       {:id 16 :name "Java Generics and Collections"  :pages 273 :edition 1 :published "2006-10-27"}]
      [{:id 21 :name "Effective Java"                 :pages 412 :edition 3 :published "2018-03-08"}
       {:id 22 :name "JCIP"                           :pages 273 :edition 1 :published "2006-10-27"}
       {:id 23 :name "The Joy of Clojure"             :pages 520 :edition 2 :published "2014-06-17"}
       {:id 24 :name "The Joy of Clojure"             :pages 320 :edition 2 :published "2014-06-17"}
       {:id 25 :name "Clojure Programming"            :pages 630 :edition 1 :published "2012-04-22"}]
      :=> {:ins [{:id 16 :name "Java Generics and Collections"  :pages 273 :edition 1 :published "2006-10-27"}
                 {:id 12 :name "Java Concurrency in Practice"   :pages 412 :edition 1 :published "2006-05-19"}
                 {:id 15 :name "Concurrent Programming in Java" :pages 422 :edition 2 :published "1999-11-04"}]
           :del [{:id 24 :name "The Joy of Clojure" :pages 320 :edition 2 :published "2014-06-17"}
                 {:id 22 :name "JCIP"               :pages 273 :edition 1 :published "2006-10-27"}]
           :upd [{:src  {:id 13 :name "The Joy of Clojure" :pages 520 :edition 2 :published "2014-06-13"}
                  :tgt  {:id 23 :name "The Joy of Clojure" :pages 520 :edition 2 :published "2014-06-17"}
                  :cols [:published]}
                 {:src  {:id 11 :name "Effective Java" :pages 414 :edition 3 :published "2018-01-06"}
                  :tgt  {:id 21 :name "Effective Java" :pages 412 :edition 3 :published "2018-03-08"}
                  :cols [:pages :published]}]}))

  (testing "Trash cases"
    (are [config src tgt _ ret]
        (st/with-instrument-disabled
          (test-diff config src tgt ret))

      ;; NONE TO NONE

      {:match-by [:foo]}                   nil nil :=> {}
      {:match-by [:foo]}                   []  nil :=> {}
      {:match-by [:foo] :ponders {:bar 1}} nil nil :=> {}
      {:match-by [:foo] :ponders {:bar 1}} []  nil :=> {}

      ;; SEQ TO NONE

      {:match-by [:foo]}
      [{:foo 1 :bar 1}]
      nil
      :=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:foo]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}]
      nil
      :=> {:ins [{:foo 1 :bar 1}]}

      {:match-by [:bar]}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      nil
      :=> {:ins [{:foo 1 :bar 1}
                 {:foo 2 :bar 2}]}

      {:match-by [:bar]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 2 :bar 2}]
      nil
      :=> {:ins [{:foo 1 :bar 1}
                 {:foo 2 :bar 2}]}

      ;; MANY TO SEQ

      ;; Overlap between match-by and ponders
      {:match-by [:foo :bar]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}]
      :=> {:ins [{:foo 1 :bar 1}
                 {:foo 1 :bar 4}]
           :del [{:foo 1 :bar 2}]}

      ;; Match-by cols missing in rows
      {:match-by [:baz]
       :ponders  {:bar 1}}
      [{:foo 1 :bar 1}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}]
      :=> {:ins [{:foo 1 :bar 4}]
           :upd [{:src  {:foo 1 :bar 1}
                  :tgt  {:foo 1 :bar 2}
                  :cols [:bar]}]}

      ;; Ponder cols missing in all rows
      {:match-by [:foo]
       :ponders  {:baz 1}}
      [{:foo 1 :bar 1}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}]
      :=> {:ins [{:foo 1 :bar 4}]}

      ;; Ponder cols missing in some rows
      {:match-by [:foo]
       :ponders  {:baz 1}}
      [{:foo 1 :bar 1 :baz 4}
       {:foo 1 :bar 4}]
      [{:foo 1 :bar 2}
       {:foo 1 :bar 7}]
      :=> {:upd [{:src  {:foo 1 :bar 1 :baz 4}
                  :tgt  {:foo 1 :bar 7}
                  :cols [:baz]}]})

    ;; Match-by is REQUIRED

    (st/with-instrument-disabled
      (is (thrown? ArityException (diff nil)))
      (is (thrown? ArityException (diff {})))
      (is (thrown? ArityException (diff {:match-by []})))))

  (testing "Transducer"
    (are [xf src-tgt _ ret]
        (= (sort-by hash ret)
           (sort-by hash (into [] xf src-tgt)))

      (diff {:match-by [:foo]
             :ponders  {:bar 1 :baz 2}})
      [[[{:id  1 :foo 1 :bar 1 :baz 1}]
        [{:id 11 :foo 1 :bar 1 :baz 1}]]]
      :=> []

      (diff {:match-by [:foo]
             :ponders  {:bar 1 :baz 2}})
      [[[{:id  1 :foo 1 :bar 1 :baz 1}
         {:id  2 :foo 2 :bar 2 :baz 2}]
        [{:id 11 :foo 1 :bar 1 :baz 3}
         {:id 22 :foo 2 :bar 2 :baz 2}]]]
      :=> [{:upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                   :cols [:baz]}]}]

      (diff {:match-by [:foo :bar]
             :ponders  {:baz 2}})
      [[[{:id  1 :foo 1 :bar 1 :baz 1}
         {:id  2 :foo 2 :bar 1 :baz 2}
         {:id  3 :foo 2 :bar 4 :baz 2}]
        [{:id 11 :foo 1 :bar 1 :baz 3}
         {:id 22 :foo 2 :bar 4 :baz 2}]]]
      :=> [{:ins [{:id 2 :foo 2 :bar 1 :baz 2}]}
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
      :=> [{:ins [{:id 2 :foo 1 :bar 1 :baz 2}
                  {:id 3 :foo 1 :bar 1 :baz 2}]
            :upd [{:src  {:id  1 :foo 1 :bar 1 :baz 1}
                   :tgt  {:id 11 :foo 1 :bar 1 :baz 3}
                   :cols [:baz]}]}
           {:del [{:id 22 :foo 2 :bar 4 :baz 2}
                  {:id 33 :foo 2 :bar 4 :baz 2}]}
           {:del [{:id 44 :foo 3 :bar 4 :baz 2}]}]

      ;; Example from doc/api.md
      (diff {:match-by [:name :edition]
             :ponders  {:pages     1
                        :published 2}})
      [[[{:id 11 :name "Effective Java"                 :pages 414 :edition 3 :published "2018-01-06"}
         {:id 12 :name "Java Concurrency in Practice"   :pages 412 :edition 1 :published "2006-05-19"}
         {:id 13 :name "The Joy of Clojure"             :pages 520 :edition 2 :published "2014-06-13"}
         {:id 14 :name "Clojure Programming"            :pages 630 :edition 1 :published "2012-04-22"}
         {:id 15 :name "Concurrent Programming in Java" :pages 422 :edition 2 :published "1999-11-04"}
         {:id 16 :name "Java Generics and Collections"  :pages 273 :edition 1 :published "2006-10-27"}]
        [{:id 21 :name "Effective Java"                 :pages 412 :edition 3 :published "2018-03-08"}
         {:id 22 :name "JCIP"                           :pages 273 :edition 1 :published "2006-10-27"}
         {:id 23 :name "The Joy of Clojure"             :pages 520 :edition 2 :published "2014-06-17"}
         {:id 24 :name "The Joy of Clojure"             :pages 320 :edition 2 :published "2014-06-17"}
         {:id 25 :name "Clojure Programming"            :pages 630 :edition 1 :published "2012-04-22"}]]]
      :=> [{:del [{:id 24 :name "The Joy of Clojure" :pages 320 :edition 2 :published "2014-06-17"}]
            :upd [{:src  {:id 13 :name "The Joy of Clojure" :pages 520 :edition 2 :published "2014-06-13"}
                   :tgt  {:id 23 :name "The Joy of Clojure" :pages 520 :edition 2 :published "2014-06-17"}
                   :cols [:published]}]}
           {:ins [{:id 16 :name "Java Generics and Collections" :pages 273 :edition 1 :published "2006-10-27"}]}
           {:ins [{:id 12 :name "Java Concurrency in Practice" :pages 412 :edition 1 :published "2006-05-19"}]}
           {:upd [{:src  {:id 11 :name "Effective Java" :pages 414 :edition 3 :published "2018-01-06"}
                   :tgt  {:id 21 :name "Effective Java" :pages 412 :edition 3 :published "2018-03-08"}
                   :cols [:pages :published]}]}
           {:ins [{:id 15 :name "Concurrent Programming in Java" :pages 422 :edition 2 :published "1999-11-04"}]}
           {:del [{:id 22 :name "JCIP" :pages 273 :edition 1 :published "2006-10-27"}]}])))
