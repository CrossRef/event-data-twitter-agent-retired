(defproject event-data-twitter-agent "0.1.0-SNAPSHOT"
  :description "Twitter agent for Crossref Event Data"
  :url "http://eventdata.crossref.org"
  :license {:name "The MIT License (MIT)"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.7.0"]]
  :main ^:skip-aot event-data-twitter-agent.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
