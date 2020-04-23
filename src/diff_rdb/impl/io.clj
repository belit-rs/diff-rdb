;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as async])
  (:import
   (java.io BufferedReader)
   (clojure.lang IReduceInit)))


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
  as an argument and f will be re-run. After all n fs have
  gracefully finished, the pool will be closed and a zero
  arity function on-close will be called."
  [n f ex-handler on-close]
  (assert (pos? n))
  (let [ch-pool (async/chan)
        xf      (map (fn [wkr] (f) wkr))
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
