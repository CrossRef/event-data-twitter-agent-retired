(defproject event-data-twitter-agent "0.1.0-SNAPSHOT"
  :description "Twitter agent for Crossref Event Data"
  :url "http://eventdata.crossref.org"
  :license {:name "The MIT License (MIT)"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [http-kit "2.1.18"]
                 [org.clojure/data.json "0.2.6"]
                 [com.twitter/hbc-core "2.2.0"]
                 [org.apache.httpcomponents/httpclient "4.5.2"]
                 [org.slf4j/slf4j-simple "1.7.21"]
                 [clj-time "0.12.0"]
                 [crossref-util "0.1.10"]
                 [robert/bruce "0.8.0"]
                 [crossref/baleen "0.1"]]
                 
  :main ^:skip-aot event-data-twitter-agent.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :prod {:resource-paths ["config/prod"]}
             :dev  {:resource-paths ["config/dev"]}})
