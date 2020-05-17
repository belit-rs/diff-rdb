;; Copyright (c) Belit d.o.o.


(ns diff-rdb.io
  (:require
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [diff-rdb.impl.util :as util]
   [diff-rdb.impl.io :as impl]))


(defn ptn
  "Given a config map and a channel for errors, returns
  a channel that yields partitions and closes when all
  partitions are loaded or if an exception occured.

  Entries of the config map [REQUIRED]:
  * :ptn/con   - Database connection specification.
  * :ptn/query - SQL query for data to be partitioned.
  * :ptn/size  - Number of elements in each partition.

  Exceptions are mapified and placed on the ch-err."
  [config ch-err]
  (let [{:ptn/keys [con query size]} config
        opts {:builder-fn rs/as-arrays}]
    (impl/reducible->chan
     (util/ptn-xf size)
     (jdbc/plan con [query] opts)
     (fn ptn-ex-handler [ex]
       (->> {:err :ptn
             :ex  (Throwable->map ex)}
            (async/put! ch-err))))))
