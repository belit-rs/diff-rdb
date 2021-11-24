;; Copyright (c) Belit d.o.o.


(ns diff-rdb.impl.io
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [next.jdbc :as jdbc]
   [next.jdbc.prepare :as prep])
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
  (let [chan (async/chan)]
    (Thread/setDefaultUncaughtExceptionHandler
     (reify Thread$UncaughtExceptionHandler
       (uncaughtException [_ _ ex]
         (when-not (async/put! chan ex)
           (Thread/setDefaultUncaughtExceptionHandler nil)
           (let [thread (Thread/currentThread)]
             (-> (.getUncaughtExceptionHandler thread)
                 (.uncaughtException thread ex)))))))
    chan))


(defn reducible->chan
  "Creates and returns a channel with the contents of
  reducible, transformed using the xform transducer.
  Channel closes when reducible is fully reduced or if
  exception is thrown (will be re-thrown as ex-info)."
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
          (->> {:err ::reducible->chan
                :ex  (Throwable->map ex)}
               (ex-info "reducible->chan failed")
               throw))))
    chan))


(defn sink-chan
  "Returns a channel that closes after n puts.
  On-close is a zero arity function.
  This channel should be put-only."
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
    (async/onto-chan! ch-pool wkrs false)))


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
                      (->> {:err ::parallel-select
                            :ptn ptn
                            :ex  (Throwable->map ex)}
                           (ex-info "parallel-select failed")
                           throw)))
              (recur))))))))


(defn drain-to-file
  "Writes values taken from a channel to a file. Each value is
  transformed to string via stringifier fn and written as a
  separate line. Creates all missing parent dirs of the file.
  Exceptions are re-thrown as ex-info."
  [file stringifier chan]
  (io/make-parents file)
  (async/thread
    (try
      (with-open [^BufferedWriter w
                  (io/writer file)]
        (loop []
          (when-some [v (async/<!! chan)]
            (.write w ^String (stringifier v))
            (.newLine w)
            (recur))))
      (catch Throwable ex
        (->> {:err  ::drain-to-file
              :file (io/file file)
              :ex   (Throwable->map ex)}
             (ex-info "drain-to-file failed")
             throw)))))
