;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.diff)


(defn pair-groups
  "Pairs values of two group-by results using vector."
  [g1 g2]
  (persistent!
   (reduce
    (fn [ret k]
      (conj! ret [(get g1 k) (get g2 k)]))
    (transient [])
    (set (into (keys g1) (keys g2))))))


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


(defn diff-rows
  "See the docstrings for `diff-rdb.diff/diff`."
  [compare-cols ponders [src tgt]]
  (let [src (zipmap (range) src)
        tgt (zipmap (range) tgt)
        upd (->> (for [[is ms] src
                       [it mt] tgt]
                   [is it (remove #(= (get ms %)
                                      (get mt %))
                                  compare-cols)])
                 (sort-by #(->> (last %)
                                (map ponders)
                                (reduce +)))
                 (distinct-by first second))
        ins (->> (map first  upd) (apply dissoc src) vals)
        del (->> (map second upd) (apply dissoc tgt) vals)
        upd (->> (filter (comp seq last) upd)
                 (map (fn [[is it cols]]
                        {:src  (get src is)
                         :tgt  (get tgt it)
                         :cols (vec cols)}))
                 seq)]
    (cond-> {}
      ins (assoc :ins (vec ins))
      del (assoc :del (vec del))
      upd (assoc :upd (vec upd)))))
