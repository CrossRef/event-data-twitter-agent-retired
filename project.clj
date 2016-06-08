(defproject event-data-twitter-agent "0.1.0-SNAPSHOT"
  :description "Twitter agent for Crossref Event Data"
  :url "http://eventdata.crossref.org"
  :license {:name "The MIT License (MIT)"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [yogthos/config "0.8"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.amazonaws/aws-java-sdk "1.11.6"]
                 [http-kit "2.1.18"]
                 [org.clojure/data.json "0.2.6"]
                 [com.twitter/hbc-core "2.2.0"]
                 [redis.clients/jedis "2.8.0"]]
  :main ^:skip-aot event-data-twitter-agent.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :prod {:resource-paths ["config/prod"]}
             :dev  {:resource-paths ["config/dev"]}})
