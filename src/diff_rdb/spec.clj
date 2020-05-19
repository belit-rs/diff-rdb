;; Copyright (c) Belit d.o.o.


(ns diff-rdb.spec
  (:require
   [clojure.spec.alpha :as s]
   [next.jdbc.specs :as jdbc]
   [diff-rdb.diff]
   [diff-rdb.io]
   [clojure.core.async.impl.channels])
  (:import (clojure.core.async.impl.channels MMC)))


;; ==============================


(s/def :async/chan #(instance? MMC %))


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


(s/def :ptn/con   ::jdbc/db-spec)
(s/def :ptn/query string?)
(s/def :ptn/size  (s/and nat-int? pos?))


(s/fdef diff-rdb.io/ptn
  :args (s/cat :config (s/keys :req [:ptn/con
                                     :ptn/query
                                     :ptn/size])
               :ch-err :async/chan)
  :ret  :async/chan)


;; ==============================


(s/def :diff/workers (s/and nat-int? pos?))
(s/def :src/con      ::jdbc/db-spec)
(s/def :tgt/con      ::jdbc/db-spec)
(s/def :src/query    string?)
(s/def :tgt/query    string?)
(s/def :src/con-opts ::jdbc/opts-map)
(s/def :tgt/con-opts ::jdbc/opts-map)
(s/def :src/pst-opts ::jdbc/opts-map)
(s/def :tgt/pst-opts ::jdbc/opts-map)
(s/def :src/exe-opts ::jdbc/opts-map)
(s/def :tgt/exe-opts ::jdbc/opts-map)


(s/fdef diff-rdb.io/diff
  :args (s/cat :config (s/keys :req-un [:diff/workers
                                        :diff/match-by]
                               :opt-un [:diff/ponders]
                               :req    [:src/con
                                        :tgt/con
                                        :src/query
                                        :tgt/query]
                               :opt    [:src/con-opts
                                        :tgt/con-opts
                                        :src/pst-opts
                                        :tgt/pst-opts
                                        :src/exe-opts
                                        :tgt/exe-opts])
               :ch-err :async/chan
               :ch-ptn :async/chan)
  :ret  :async/chan)


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


(comment ;; TODO spec2 + select
  (s/def ::config
    (s/keys :opt-un [:diff/workers
                     :diff/match-by
                     :diff/ponders]
            :opt    [:ptn/con
                     :ptn/query
                     :ptn/size                     
                     :src/con
                     :src/query
                     :src/con-opts
                     :src/pst-opts
                     :src/exe-opts
                     :tgt/con
                     :tgt/query
                     :tgt/con-opts
                     :tgt/pst-opts
                     :tgt/exe-opts])))
