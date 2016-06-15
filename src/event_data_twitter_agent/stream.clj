(ns event-data-twitter-agent.stream
  "Handle Gnip's stream and put into Redis."
  (:require [config.core :refer [env]])
  (:require [event-data-twitter-agent.util :as util])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [com.twitter.hbc.httpclient.auth BasicAuth]
           [com.twitter.hbc ClientBuilder]
           [com.twitter.hbc.core Constants]
           [com.twitter.hbc.core.processor LineStringProcessor]
           [com.twitter.hbc.core.endpoint RealTimeEnterpriseStreamingEndpoint]
           [redis.clients.jedis Jedis])
  (:gen-class))

(defn- parse-entry
  "Parse a tweet input (JSON String) into a standard format. A single map with keys:
   - tweetId - string id
   - postedTime - string ISO8601 Z directly from twitter
   - postedDate - string ISO8601 Z truncated to day
   - body - body text
   - urls - list of URLs
   - matchingRules - list or Gnip rules that resulted in the match, for diagnostic purposes"
  [input-string]
  (let [parsed (json/read-str input-string)
        posted-time (get-in parsed ["postedTime"])
        year-month-day (.substring posted-time 0 10)
        urls (map #(get % "expanded_url") (get-in parsed ["gnip" "urls"]))
        matching-rules (map #(get % "value") (get-in parsed ["gnip" "matching_rules"]))]
  {"tweetId" (get parsed "id")
   "author" (get-in parsed ["actor" "link"])
   "postedTime" posted-time
   "postedDate" year-month-day
   "body" (get parsed "body")
   "urls" urls
   "matchingRules" matching-rules}))

(defn run
  "Run the stream ingestion.
  This pushes events onto two lists:
   - 'input-queue' - a queue for processing
   - 'input-log-YYYY-MM-DD' - the log of inputs. This is written to a log file.
  Blocks forever."
  []
  (let [^Jedis redis-conn (util/jedis-connection)
        q (new LinkedBlockingQueue 1000) 
        client (-> (new ClientBuilder)
                   (.hosts Constants/ENTERPRISE_STREAM_HOST)
                   (.endpoint (new RealTimeEnterpriseStreamingEndpoint "Crossref" "track" "prod"))
                   (.authentication (new BasicAuth (:gnip-username env) (:gnip-password env)))
                   (.processor (new com.twitter.hbc.core.processor.LineStringProcessor q))
                   (.build))]
        (l/info "Connecting to Gnip...")
        (.connect client)
        (l/info "Connected to Gnip.")

        (loop []
          ; Block on the take.
          (let [event (.take q)
                parsed (parse-entry event)
                input-bucket-key (str "input-log-" (get parsed "postedDate"))
                serialized (json/write-str parsed)]
            
            ; Push to start for queue.
            (.lpush redis-conn "input-queue" (into-array [serialized]))

            ; Push to end for log.
            (.rpush redis-conn input-bucket-key (into-array [serialized])))
          (recur))))