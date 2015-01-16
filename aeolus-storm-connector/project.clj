(defproject aeolus-storm-connector "1.0-SNAPSHOT"
  :description "Provides a bridge to call Storm's hash-function used for field grouping."
  :url "http://u.hu-berlin.de/mjsax"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.storm/storm-core "0.9.2-incubating"]]
  :aot :all)
