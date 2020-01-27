;; Copyright (c) Belit d.o.o.


(ns diff-rdb.diff
  (:require [diff-rdb.impl.diff :as impl]))


(defn diff
  "Given a config map, returns the difference between
  src (source) and tgt (target) data sets, including:
  * :ins - Rows from src missing in tgt.
  * :del - Rows from tgt missing in src.
  * :upd - Rows in both src and tgt but with different
           values in pondered columns (see below).

  Entries of the config map:
  * :match-by  - A non-empty collection of columns which
    [REQUIRED]   are used to group src and tgt rows and
                 pair those groups for comparison.
  * :ponders   - A map of column to number (ponder / weight).
    [OPTIONAL]   Rows are compared only on these columns.
                 Ponders are used to resolve cases with
                 one-to-many, many-to-one or many-to-many
                 rows in (match-by) groups for comparison.
                 Rows are paired based on the sum of ponders
                 of all of their differing columns.
                 Pairs with the smallest sum are chosen and
                 listed as :upd. Unpaired rows from src / tgt
                 are listed as :ins / :del respectively.

  Returns a transducer for the 1-parameter arity."
  ([config]
   (let [{:keys [match-by ponders]} config
         match-fn (apply juxt match-by)
         compare-cols (keys ponders)]
     (comp (map (fn [[src tgt]]
                  (impl/pair-groups
                   (group-by match-fn src)
                   (group-by match-fn tgt))))
           (mapcat vals)
           (map #(impl/remove-eq-rows compare-cols %))
           (filter #(some some? %))
           (map #(impl/diff-rows compare-cols ponders %)))))
  ([config src tgt]
   (transduce (diff config)
              (partial merge-with into)
              {} [[src tgt]])))
