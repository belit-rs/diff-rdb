;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.diff)


(defn pair-groups
  "Pairs two group-by results using vector. Note:
  (merge-with vector {:foo 5} {}) => {:foo 5}
  (pair-groups       {:foo 5} {}) => {:foo [5 nil]}"
  [g1 g2]
  (reduce
   (fn [ret k]
     (assoc ret k [(get g1 k) (get g2 k)]))
   {} (set (into (keys g1) (keys g2)))))


(defn distinct-by
  "Returns a vector of the elements of coll with duplicates
  removed. Two elements are equal if EITHER f or g function
  returns the same value when applied to each of them."
  [f g coll]
  (loop [ret [], coll coll]
    (if-let [head (first coll)]
      (let [x (f head)
            y (g head)]
        (recur (conj ret head)
               (remove #(or (= x (f %))
                            (= y (g %)))
                       (rest coll))))
      ret)))


(defn remove-eq-rows
  "Removes rows with the same values for compare-cols in both
  src and tgt. Number of rows removed from src will match the
  number of rows removed from tgt."
  [compare-cols [src tgt]]
  (let [src (zipmap (range) src)
        tgt (zipmap (range) tgt)
        del (->> (for [[is ms] src
                       [it mt] tgt
                       :when (= (map ms compare-cols)
                                (map mt compare-cols))]
                   [is it])
                 (distinct-by first second))]
    [(->> (map first  del) (apply dissoc src) vals)
     (->> (map second del) (apply dissoc tgt) vals)]))


(defn diff-cols
  "Returns a vector of those compare-cols which are keys
  to different values in m1 and m2 maps."
  [compare-cols m1 m2]
  (filterv #(not= (get m1 %) (get m2 %)) compare-cols))


(defn diff-rows
  "Returns the difference between src and tgt groups of rows.
  Expects seq src OR tgt and no equal (by compare-cols) rows.
  See also `diff-rdb.diff/diff`."
  [compare-cols ponders [src tgt]]
  (let [src (zipmap (range) src)
        tgt (zipmap (range) tgt)
        upd (->> (for [[is ms] src
                       [it mt] tgt]
                   [is it (diff-cols compare-cols ms mt)])
                 (sort-by #(->> (last %)
                                (map ponders)
                                (reduce +)))
                 (distinct-by first second))
        ins (->> (map first  upd) (apply dissoc src) vals)
        del (->> (map second upd) (apply dissoc tgt) vals)
        upd (seq (map (fn [[is it cols]]
                        {:src  (get src is)
                         :tgt  (get tgt it)
                         :cols cols}) upd))]
    (cond-> {}
      ins (assoc :ins (vec ins))
      del (assoc :del (vec del))
      upd (assoc :upd (vec upd)))))
