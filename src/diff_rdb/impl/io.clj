;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as async])
  (:import
   (java.io File BufferedReader)
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
  exception is thrown. Exmap-handler is a function that
  handles a mapified Throwable."
  [xform reducible exmap-handler]
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
          (-> (Throwable->map ex)
              exmap-handler))
        (finally
          (async/close! chan))))
    chan))


(defn into-unordered
  "Takes elements from the from channel, applies the f
  function to each element and supplies them to the to
  channel, with parallelism n. The to channel will be
  closed when the from channel closes. Exmap-handler
  is a function that handles a mapified Throwable with
  the [:capture value-taken-from-channel] entry conj'd.
  Blocking operations are used, n threads are spawned."
  [n to f from exmap-handler]
  (let [remain (atom n)]
    (dotimes [_ n]
      (async/thread
        (loop []
          (if-some [v (async/<!! from)]
            (do (try
                  (async/>!! to (f v))
                  (catch Throwable ex
                    (-> (Throwable->map ex)
                        (assoc :capture v)
                        exmap-handler)))
                (recur))
            (when (zero? (swap! remain dec))
              (async/close! to))))))))
