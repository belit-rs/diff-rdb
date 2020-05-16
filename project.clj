;; Copyright (c) Belit d.o.o.


(defproject belit/diff-rdb "0.2.0-SNAPSHOT"
  :description "Tools and APIs for finding, inspecting and fixing differences between data sets in relational databases."
  :url "https://github.com/belit-rs/diff-rdb"
  :license {:url "https://github.com/belit-rs/diff-rdb/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.2.603"]
                 [seancorfield/next.jdbc "1.0.424"]]
  :profiles {:dev
             {:dependencies [[org.clojure/test.check "1.0.0"]
                             [expound "0.8.4"]
                             [org.postgresql/postgresql "42.2.12"]]
              :jvm-opts ["-Dclojure.core.async.go-checking=true"]}})
