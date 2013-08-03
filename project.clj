(defproject comanche "0.1.0"
  :description "Cluster leader election utility"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.rmoquin.bundle/jeromq "0.2.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.taoensso/timbre "2.3.0"] ]
  :main comanche.draft)
