;; Copyright (c) Belit d.o.o.


(ns diff-rdb.doc.api-test
  (:require [clojure.test :refer :all]))


(def src
  [{:id 11 :name "Effective Java"                 :pages 414 :edition 3 :published "2018-01-06"}
   {:id 12 :name "Java Concurrency in Practice"   :pages 412 :edition 1 :published "2006-05-19"}
   {:id 13 :name "The Joy of Clojure"             :pages 520 :edition 2 :published "2014-06-13"}
   {:id 14 :name "Clojure Programming"            :pages 630 :edition 1 :published "2012-04-22"}
   {:id 15 :name "Concurrent Programming in Java" :pages 422 :edition 2 :published "1999-11-04"}
   {:id 16 :name "Java Generics and Collections"  :pages 273 :edition 1 :published "2006-10-27"}])


(def tgt
  [{:id 21 :name "Effective Java"                 :pages 412 :edition 3 :published "2018-03-08"}
   {:id 22 :name "JCIP"                           :pages 273 :edition 1 :published "2006-10-27"}
   {:id 23 :name "The Joy of Clojure"             :pages 520 :edition 2 :published "2014-06-17"}
   {:id 24 :name "The Joy of Clojure"             :pages 320 :edition 2 :published "2014-06-17"}
   {:id 25 :name "Clojure Programming"            :pages 630 :edition 1 :published "2012-04-22"}])


(require '[diff-rdb.diff :refer [diff]])


(deftest diff-test
  (is (= (diff {:match-by [:name :edition]
                :ponders  {:pages     1
                           :published 2}}
               src tgt)

         { ;; Rows from src missing in tgt
          :ins [{:id 16 :name "Java Generics and Collections"  :pages 273 :edition 1 :published "2006-10-27"}
                {:id 12 :name "Java Concurrency in Practice"   :pages 412 :edition 1 :published "2006-05-19"}
                {:id 15 :name "Concurrent Programming in Java" :pages 422 :edition 2 :published "1999-11-04"}]

          ;; Rows from tgt missing in src
          :del [{:id 24 :name "The Joy of Clojure" :pages 320 :edition 2 :published "2014-06-17"}
                {:id 22 :name "JCIP"               :pages 273 :edition 1 :published "2006-10-27"}]

          ;; Rows in both src and tgt but with different values in pondered columns
          :upd [{:src  {:id 13 :name "The Joy of Clojure" :pages 520 :edition 2 :published "2014-06-13"}
                 :tgt  {:id 23 :name "The Joy of Clojure" :pages 520 :edition 2 :published "2014-06-17"}
                 :cols [:published]}
                {:src  {:id 11 :name "Effective Java" :pages 414 :edition 3 :published "2018-01-06"}
                 :tgt  {:id 21 :name "Effective Java" :pages 412 :edition 3 :published "2018-03-08"}
                 :cols [:pages :published]}]})))


(def xf
  (diff {:match-by [:name :edition]
         :ponders  {:pages     1
                    :published 2}}))


(deftest diff-transducer-test
  (is (= (into [] xf [[src tgt]])

         [{:del [{:id 24 :name "The Joy of Clojure" :pages 320 :edition 2 :published "2014-06-17"}]
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
