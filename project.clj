;; Copyright (c) Belit d.o.o.


(defproject belit/diff-rdb "0.2.0-SNAPSHOT"
  :description "Tools and APIs for finding, inspecting and fixing differences between data sets in relational databases."
  :url "https://github.com/belit-rs/diff-rdb"
  :license {:url "https://github.com/belit-rs/diff-rdb/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.0.567"]
                 [seancorfield/next.jdbc "1.0.13"]]
  :profiles {:dev
             {:dependencies [[org.postgresql/postgresql "42.2.10"]]
              :jvm-opts ["-Dclojure.core.async.go-checking=true"]
              :global-vars {*warn-on-reflection* true}
              :injections [(require 'next.jdbc.specs)
                           (next.jdbc.specs/instrument)]}})
