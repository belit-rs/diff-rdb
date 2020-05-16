;; Copyright (c) Belit d.o.o.


(ns diff-rdb.spec
  (:require
   [clojure.spec.alpha :as s]
   [diff-rdb.diff]))


;; ==============================


(s/def :db/col  keyword?)
(s/def :db/cols (s/coll-of :db/col
                           :min-count 1
                           :kind sequential?))
(s/def :db/row  (s/map-of :db/col any?))
(s/def :db/rows (s/coll-of :db/row
                           :kind sequential?))


;; ==============================


(s/def :diff/match-by :db/cols)
(s/def :diff/ponders  (s/map-of :db/col number?))
(s/def :diff/config   (s/keys :req-un [:diff/match-by]
                              :opt-un [:diff/ponders]))


(s/def :diff.upd/src  :db/row)
(s/def :diff.upd/tgt  :db/row)
(s/def :diff.upd/cols :db/cols)


(s/def :diff/ins :db/rows)
(s/def :diff/del :db/rows)
(s/def :diff/upd (s/coll-of
                  (s/keys :req-un [:diff.upd/src
                                   :diff.upd/tgt
                                   :diff.upd/cols])))


(s/fdef diff-rdb.diff/diff
  :args (s/alt :transducer :diff/config
               :transduced (s/cat :config :diff/config
                                  :src    :db/rows
                                  :tgt    :db/rows))
  :ret  (s/or  :transducer fn?
               :transduced (s/keys :opt-un [:diff/ins
                                            :diff/del
                                            :diff/upd])))


;; ==============================


(comment ;; TODO spec2 + select
  (s/def ::config
    (s/keys :opt-un [:diff/match-by
                     :diff/ponders])))
