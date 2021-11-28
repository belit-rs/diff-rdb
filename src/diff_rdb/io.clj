;; Copyright (c) Belit d.o.o.


(ns diff-rdb.io
  (:require
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [diff-rdb.diff :as core]
   [diff-rdb.impl.util :as util]
   [diff-rdb.impl.io :as impl]))


(defn ptn
  "Given a config map, returns a channel that yields partitions and
  closes when all partitions are loaded or if exception is thrown.

  REQUIRED entries of the config map:
  * :ptn/size - Number of elements in each partition.
  * :ptn/plan - Map with REQUIRED entries:
                * :conn  - Database connection specification.
                * :query - SQL query for data to be partitioned."
  [config]
  (let [{:keys [conn query]} (:ptn/plan config)
        size                 (:ptn/size config)
        opts {:builder-fn rs/as-arrays}]
    (impl/reducible->chan
     (util/ptn-xf size)
     (jdbc/plan conn [query] opts))))


(defn diff
  "Given a config map and a channel that yields partitions, returns
  a channel that yields differences between src (source) and tgt
  (target) data sets selected for each partition. Returned channel
  closes after ch-ptn is closed and drained or if unrecoverable
  exception is thrown by each worker thread. Exception is recoverable
  if it has :ptn (data taken from the ch-ptn) captured in the ex-data.

  Entries of the config map:
    [REQUIRED]
  * :workers  - Number of concurrent processes that will take
                ptns and select data (src and tgt queries are
                executed in parallel, too); number of src and
                tgt db connections and prepared statements to
                be opened and reused.
  * :match-by - See `diff-rdb.diff/diff`.
  * :src/plan - Map with REQUIRED entries:
  * :tgt/plan / * :conn  - Database connection specification.
                * :query - SQL query for data to be diff'd.
                And OPTIONAL entries:
                * :opts  - Map with all jdbc options.
    [OPTIONAL]
  * :ponders - See `diff-rdb.diff/diff`."
  [config ch-ptn]
  (let [wkrs    (:workers config)
        ch-data (async/chan wkrs)
        xf      (core/diff config)
        ch-diff (async/chan wkrs)
        cores   (-> (Runtime/getRuntime)
                    .availableProcessors inc)]
    (async/pipeline cores ch-diff xf ch-data)
    (impl/run-in-pool wkrs
                      (impl/parallel-select-fn
                       (:src/plan config)
                       (:tgt/plan config)
                       ch-ptn ch-data)
                      (fn diff-ex-recoverable? [ex]
                        (-> ex ex-data :ptn boolean))
                      (fn diff-on-close []
                        (async/close! ch-data)))
    ch-diff))


(defn split-by-diff
  "Returns a map with channels subscribed to the ch-diff:
  * :ins - Channel that receives :ins rows.
  * :del - Channel that receives :del rows.
  * :upd - Channel that receives :upd rows.

  Each channel will have sub-buf buffer and each will
  close after the ch-diff is closed and drained."
  [sub-buf ch-diff]
  (let [ch-cat (async/chan sub-buf cat)
        ch-pub (async/pub ch-cat first)
        xf-sub (mapcat second)
        ch-ins (async/chan sub-buf xf-sub)
        ch-del (async/chan sub-buf xf-sub)
        ch-upd (async/chan sub-buf xf-sub)]
    (async/sub ch-pub :ins ch-ins)
    (async/sub ch-pub :del ch-del)
    (async/sub ch-pub :upd ch-upd)
    (async/pipe ch-diff ch-cat)
    {:ins ch-ins :del ch-del :upd ch-upd}))
