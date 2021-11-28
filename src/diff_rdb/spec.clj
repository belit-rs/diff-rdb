;; Copyright (c) Belit d.o.o.


(ns diff-rdb.spec
  (:require
   [clojure.string :as str]
   [clojure.spec.alpha :as s]
   [next.jdbc.specs :as jdbc]
   [diff-rdb.diff :as diff]
   [diff-rdb.io :as io]
   [clojure.core.async.impl.channels])
  (:import (clojure.core.async.impl.protocols Channel Buffer)))


;; ==============================


(s/def ::not-blank-string
  (s/and string?
         (complement str/blank?)))


(s/def ::mapified-throwable
  (s/map-of any? any?
            :min-count 1))


(s/def ::chan #(instance? Channel %))
(s/def ::buf  #(instance? Buffer  %))


(s/def ::col  keyword?)
(s/def ::cols (s/coll-of ::col
                         :min-count 1
                         :kind sequential?))


(s/def ::row  (s/map-of ::col any?))
(s/def ::rows (s/coll-of ::row
                         :kind sequential?))


(s/def ::val  any?)
(s/def ::vals (s/coll-of ::val
                         :kind sequential?))


(s/def ::match-by ::cols)
(s/def ::ponders  (s/map-of ::col number?))


(s/def ::conn  ::jdbc/connectable)
(s/def ::query ::not-blank-string)
(s/def ::opts  ::jdbc/opts-map)
(s/def ::plan  (s/keys :req-un [::conn
                                ::query]
                       :opt-un [::opts]))


(s/def ::workers (s/and nat-int? pos?))


;; ==============================


(s/def ::diff/config
  (s/keys :req-un [::match-by]
          :opt-un [::ponders]))


(s/def ::diff/src  ::row)
(s/def ::diff/tgt  ::row)
(s/def ::diff/cols ::cols)


(s/def ::diff/ins ::rows)
(s/def ::diff/del ::rows)
(s/def ::diff/upd (s/coll-of
                   (s/keys :req-un [::diff/src
                                    ::diff/tgt
                                    ::diff/cols])))


(s/fdef diff-rdb.diff/diff
  :args (s/alt :transducer ::diff/config
               :transduced (s/cat :config ::diff/config
                                  :src    ::rows
                                  :tgt    ::rows))
  :ret  (s/or  :transducer fn?
               :transduced (s/keys :opt-un [::diff/ins
                                            ::diff/del
                                            ::diff/upd])))


;; ==============================


(s/def :ptn/size (s/and nat-int? pos?))
(s/def :ptn/plan ::plan)


(s/fdef diff-rdb.io/ptn
  :args (s/cat :config (s/keys :req [:ptn/size
                                     :ptn/plan]))
  :ret  ::chan)


;; ==============================


(s/def :src/plan ::plan)
(s/def :tgt/plan ::plan)


(s/fdef diff-rdb.io/diff
  :args (s/cat :config (s/keys :req-un [::workers
                                        ::match-by]
                               :opt-un [::ponders]
                               :req    [:src/plan
                                        :tgt/plan])
               :ch-ptn ::chan)
  :ret  ::chan)


;; ==============================


(s/def ::io/ins ::chan)
(s/def ::io/del ::chan)
(s/def ::io/upd ::chan)


(s/fdef diff-rdb.io/split-by-diff
  :args (s/cat :sub-buf (s/or :buf ::buf
                              :int (s/and nat-int? pos?))
               :ch-diff ::chan)
  :ret  (s/keys :req-un [::io/ins
                         ::io/del
                         ::io/upd]))


;; ==============================


(s/def ::err     keyword?)
(s/def ::ex      ::mapified-throwable)
(s/def ::ptn     ::vals)
(s/def ::ex-data (s/keys :req-un [::err
                                  ::ex]
                         :opt-un [::ptn]))


;; ==============================
