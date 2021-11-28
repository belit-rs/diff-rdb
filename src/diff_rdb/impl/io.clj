;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [next.jdbc.prepare :as prep]
   [diff-rdb.impl.util :as util])
  (:import
   (java.io BufferedReader
            BufferedWriter)
   (clojure.lang IReduceInit)))


(defn <??
  "Variation of the `clojure.core.async/<!!` that throws
  an exception if val taken from the channel is Throwable."
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
    (reduce [_ f init]
      (with-open [^BufferedReader r
                  (io/reader file)]
        (loop [state init]
          (if (reduced? state)
            state
            (if-let [line (.readLine r)]
              (recur (f state line))
              state)))))))


(defn uncaught-ex-chan
  "Returns a channel with exceptions caught by the default
  uncaught exception handler. When this channel is closed,
  next uncaught exception will cause default handler's
  behaviour to be restored, i.e. it will be set to nil."
  []
  (let [chan (async/chan 1024)]
    (Thread/setDefaultUncaughtExceptionHandler
     (reify Thread$UncaughtExceptionHandler
       (uncaughtException [_ _ ex]
         (when-not (async/>!! chan ex)
           (Thread/setDefaultUncaughtExceptionHandler nil)
           (util/thread-handle-uncaught-ex ex)))))
    chan))


(defn reducible->chan
  "Creates and returns a channel with the contents of
  reducible, transformed using the xform transducer.
  Channel closes when reducible is fully reduced or if
  exception is thrown."
  [xform reducible]
  (let [chan (async/chan)]
    (async/thread
      (try
        (transduce
         xform
         (fn drain-chan
           ([_ v] (when-not (async/>!! chan v)
                    (reduced reducible)))
           ([_]   (async/close! chan)))
         nil reducible)
        (catch Throwable ex
          (async/close! chan)
          (throw ex))))
    chan))


(defn run-in-pool
  "Runs a zero arity function f in parallel, on n threads.
  If f throws a recoverable exception, it will be re-executed.
  After all threads have finished, the pool will be closed
  and a zero arity function on-close will be called."
  [n f ex-recoverable? on-close]
  (assert (pos? n))
  (let [pool (async/pipeline-blocking
              n
              (async/chan n)
              (map (fn pool-f [wkr]
                     (if ((every-pred some? ex-recoverable?)
                          (try (f) nil
                               (catch Throwable ex
                                 (util/thread-handle-uncaught-ex ex)
                                 ex)))
                       (recur wkr)
                       wkr)))
              (async/to-chan (repeat n :wkr)))]
    (async/thread (async/<!! pool) (on-close))))


(defn async-select
  "Executes pst PreparedStatement with ptn parameters in another
  thread. Returns a channel which will receive the result of the
  execution, when completed, then close. Exception, if thrown,
  is put on the returned channel."
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
  exception is thrown (failed ptn is captured in the ex-data)."
  [plan-src plan-tgt ch-ptn ch-out]
  (let [con-src (:conn plan-src)
        con-tgt (:conn plan-tgt)
        opt-src (:opts plan-src {})
        opt-tgt (:opts plan-tgt {})
        qry-src [(:query plan-src)]
        qry-tgt [(:query plan-tgt)]]
    (fn parallel-select []
      (with-open [con-src (jdbc/get-connection con-src opt-src)
                  con-tgt (jdbc/get-connection con-tgt opt-tgt)
                  pst-src (jdbc/prepare con-src qry-src opt-src)
                  pst-tgt (jdbc/prepare con-tgt qry-tgt opt-tgt)]
        (loop []
          (when-some [ptn (async/<!! ch-ptn)]
            (when (try
                    (let [src (async-select pst-src ptn opt-src)
                          tgt (async-select pst-tgt ptn opt-tgt)
                          src (<?? src)
                          tgt (<?? tgt)]
                      (async/>!! ch-out [src tgt]))
                    (catch Throwable ex
                      (throw (ex-info "parallel-select failed"
                                      {:ptn ptn}
                                      ex))))
              (recur))))))))


(defn drain-to-file
  "Writes values taken from a channel to a file. Each value is
  transformed to string via stringifier fn and written as a
  separate line. Creates all missing parent dirs of the file."
  [file stringifier chan]
  (io/make-parents file)
  (async/thread
    (with-open [^BufferedWriter w
                (io/writer file)]
      (loop []
        (when-some [v (async/<!! chan)]
          (.write w ^String (stringifier v))
          (.newLine w)
          (recur))))))
