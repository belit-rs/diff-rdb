;; Copyright (c) Belit d.o.o.


(defproject belit/diff-rdb "0.2.0-SNAPSHOT"
  :description "Tools and APIs for finding, inspecting and fixing differences between data sets in relational databases."
  :url "https://github.com/belit-rs/diff-rdb"
  :license {:url "https://github.com/belit-rs/diff-rdb/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [org.clojure/core.async "1.8.741"]
                 [seancorfield/next.jdbc "1.2.659"]]
  :profiles {:dev
             {:dependencies [[org.clojure/test.check "1.1.1"]
                             [expound "0.9.0"]
                             [orchestra "2021.01.01-1"]
                             [com.h2database/h2 "2.3.232"]
                             [criterium "0.4.6"]]
              :jvm-opts ["-Dclojure.core.async.go-checking=true"]}})
