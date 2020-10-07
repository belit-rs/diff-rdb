;; Copyright (c) Belit d.o.o.


(ns diff-rdb.spec
  (:require
   [clojure.string :as str]
   [clojure.spec.alpha :as s]
   [next.jdbc.specs :as jdbc]
   [diff-rdb.diff]
   [diff-rdb.io]
   [clojure.core.async.impl.channels])
  (:import (clojure.core.async.impl.protocols Channel Buffer)))


;; ==============================


(s/def ::not-blank-string
  (s/and string?
         (complement str/blank?)))


(s/def :async/chan #(instance? Channel %))
(s/def :async/buf  #(instance? Buffer  %))


(s/def :db/col  keyword?)
(s/def :db/cols (s/coll-of :db/col
                           :min-count 1
                           :kind sequential?))


(s/def :db/row  (s/map-of :db/col any?))
(s/def :db/rows (s/coll-of :db/row
                           :kind sequential?))


(s/def :db/val  any?)
(s/def :db/vals (s/coll-of :db/val
                           :kind sequential?))


(s/def :db/conn  ::jdbc/connectable)
(s/def :db/query ::not-blank-string)
(s/def :db/opts  ::jdbc/opts-map)
(s/def :db/plan  (s/keys :req-un [:db/conn
                                  :db/query]
                         :opt-un [:db/opts]))


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


(s/def :ptn/size (s/and nat-int? pos?))
(s/def :ptn/plan :db/plan)


(s/fdef diff-rdb.io/ptn
  :args (s/cat :config (s/keys :req [:ptn/size
                                     :ptn/plan]))
  :ret  :async/chan)


;; ==============================


(s/def :diff/workers (s/and nat-int? pos?))
(s/def :src/plan     :db/plan)
(s/def :tgt/plan     :db/plan)


(s/fdef diff-rdb.io/diff
  :args (s/cat :config (s/keys :req-un [:diff/workers
                                        :diff/match-by]
                               :opt-un [:diff/ponders]
                               :req    [:src/plan
                                        :tgt/plan])
               :ch-err :async/chan
               :ch-ptn :async/chan)
  :ret  :async/chan)


;; ==============================


(s/def :split/ins :async/chan)
(s/def :split/del :async/chan)
(s/def :split/upd :async/chan)


(s/fdef diff-rdb.io/split-by-diff
  :args (s/cat :sub-buf (s/or :buf :async/buf
                              :int (s/and nat-int? pos?))
               :ch-diff :async/chan)
  :ret  (s/keys :req-un [:split/ins
                         :split/del
                         :split/upd]))


;; ==============================


(s/def :mapified/throwable map?)


(s/def :err.ptn/err #{:ptn})
(s/def :err.ptn/ex  :mapified/throwable)
(s/def :err/ptn     (s/keys :req-un [:err.ptn/err
                                     :err.ptn/ex]))


(s/def :err.diff/err #{:parallel-select})
(s/def :err.diff/ptn :db/vals)
(s/def :err.diff/ex  :mapified/throwable)
(s/def :err/diff     (s/keys :req-un [:err.diff/err
                                      :err.diff/ptn
                                      :err.diff/ex]))


;; ==============================
