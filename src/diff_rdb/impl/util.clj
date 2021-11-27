;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.util
  (:require
   [clojure.string :as str]))


(defn equalize-ptn
  "If the number of items in ptn partition is less
  than size, the first item will be re-added into
  it n times to make it equal."
  [size ptn]
  (let [n (- size (count ptn))]
    (if (zero? n)
      ptn
      (->> (first ptn)
           (repeat n)
           (into ptn)))))


(defn ptn-xf
  "Returns a transducer that transforms incoming
  collections of any size to vectors of size n."
  [n]
  (comp cat
        (partition-all n)
        (map #(equalize-ptn n %))))


(defn expand-?s
  "Replaces '?s' in a query with n times repeated '?',
  separated by the comma separator."
  [n query]
  (->> (repeat n \?)
       (str/join \,)
       (str/replace query "?s")))


(defn thread-handle-uncaught-ex
  "Calls uncaught exception handler of the current
  thread with the ex provided."
  [ex]
  (let [thread (Thread/currentThread)]
    (-> (.getUncaughtExceptionHandler thread)
        (.uncaughtException thread ex))))
