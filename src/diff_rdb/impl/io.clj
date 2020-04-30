;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [next.jdbc.prepare :as prep]
   [next.jdbc.result-set :as rs]
   [diff-rdb.impl.util :as util])
  (:import
   (java.io BufferedReader)
   (clojure.lang IReduceInit)))


(defn <??
  "Variation of the `clojure.core.async/<!!` that throws
  an exception if val taken from the chan is Throwable."
  [chan]
  (let [v (async/<!! chan)]
    (if (instance? Throwable v)
      (throw v)
      v)))


(defn reducible-lines
  "Returns a reducible that, when reduced, opens a reader
  and yields file's lines. Reader is closed when reduced?
  / there are no more lines to read / exception is thrown."
  [file]
  (reify IReduceInit
    (reduce [this f init]
      (with-open [^BufferedReader r
                  (io/reader file)]
        (loop [state init]
          (if (reduced? state)
            state
            (if-let [line (.readLine r)]
              (recur (f state line))
              state)))))))


(defn reducible->chan
  "Creates and returns a channel with the contents of
  reducible, transformed using the xform transducer.
  Channel closes when reducible is fully reduced or if
  exception is thrown. Ex-handler is a function of one
  argument - if an exception occurs it will be called
  with the Throwable as an argument."
  [xform reducible ex-handler]
  (let [chan (async/chan)]
    (async/thread
      (try
        (transduce
         xform
         (fn drain-chan
           ([_ v] (async/>!! chan v))
           ([_]   (async/close! chan)))
         nil reducible)
        (catch Throwable ex
          (async/close! chan)
          (ex-handler ex))))
    chan))


(defn sink-chan
  "Returns a channel that closes after n puts.
  On-close is a zero arity function.
  This channel should be write-only."
  [n on-close]
  (let [chan (async/chan)]
    (async/go-loop [n n]
      (if (pos? n)
        (if-some [_ (async/<! chan)]
          (recur (dec n))
          (on-close))
        (do (async/close! chan)
            ;; unblock pending
            (async/poll! chan)
            (on-close))))
    chan))


(defn run-in-pool
  "Runs a zero arity function f in parallel, on n threads.
  If f throws, ex-handler will be called with the Throwable
  as an argument and f will be re-executed. After all n fs
  have gracefully finished, the pool will be closed and a
  zero arity function on-close will be called."
  [n f ex-handler on-close]
  (assert (pos? n))
  (let [ch-pool (async/chan)
        xf      (map (fn pool-f [wkr] (f) wkr))
        ch-sink (sink-chan
                 n
                 (fn pool-on-close []
                   (async/close! ch-pool)
                   (on-close)))
        wkrs    (repeat n ::wkr)]
    (async/pipeline-blocking
     n ch-sink xf ch-pool true
     (fn pool-ex-handler [ex]
       (ex-handler ex)
       ;; avoid >!! deadlock
       (async/put! ch-pool ::wkr)
       nil))
    (async/onto-chan ch-pool wkrs false)))


(def default-opts
  {:con {:read-only   true
         :auto-commit false}
   :pst {:cursors     :close
         :concurrency :read-only
         :result-type :forward-only}
   :exe {:builder-fn  rs/as-unqualified-lower-maps}})


(defn async-select
  "Executes the pst PreparedStatement with the ptn parameters
  in another thread. Returns a channel which will receive the
  result of the execution when completed, then close.
  Exception, if thrown, is placed on the returned channel."
  [pst ptn opts]
  (-> (prep/set-parameters pst ptn)
      (jdbc/execute! nil opts)
      (try (catch Throwable ex ex))
      async/thread))


(defn parallel-select-fn
  "Returns a zero arity function that opens src and tgt dbase
  connections, executes src and tgt select queries in parallel
  for each partition taken from the ch-ptn channel, pairs the
  results using vector and puts pairs to the ch-out channel.
  Function loops until either ch-ptn or ch-out is closed or if
  exception is thrown (failed ptn is captured in the ex-data).
  Default opts for Connection, PreparedStatement and ResultSet
  are taken from the `diff-rdb.impl.io/default-opts` var."
  [config ch-ptn ch-out]
  (let [conn-src (:src/conn config)
        conn-tgt (:tgt/conn config)
        ptn-size (:ptn/size config)
        qry-src  [(util/expand-?s ptn-size (:src/query config))]
        qry-tgt  [(util/expand-?s ptn-size (:tgt/query config))]
        conopts  (merge (:con default-opts) (:con/opts config))
        pstopts  (merge (:pst default-opts) (:pst/opts config))
        exeopts  (merge (:exe default-opts) (:exe/opts config))]
    (fn parallel-select []
      (with-open [con-src (jdbc/get-connection conn-src conopts)
                  con-tgt (jdbc/get-connection conn-tgt conopts)
                  pst-src (jdbc/prepare con-src qry-src pstopts)
                  pst-tgt (jdbc/prepare con-tgt qry-tgt pstopts)]
        (loop []
          (when-some [ptn (async/<!! ch-ptn)]
            (when (try
                    (let [src (async-select pst-src ptn exeopts)
                          tgt (async-select pst-tgt ptn exeopts)
                          src (<?? src)
                          tgt (<?? tgt)]
                      (async/>!! ch-out [src tgt]))
                    (catch Throwable ex
                      (->> {:err :parallel-select
                            :ptn ptn
                            :ex  (Throwable->map ex)}
                           (ex-info "parallel-select failed")
                           throw)))
              (recur))))))))
