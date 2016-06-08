(ns event-data-twitter-agent.stream
  "Handle Gnip's stream and put into Redis."
  (:require [config.core :refer [env]])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest])
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
   - urls - list of URLs"
  [input-string]
  (let [parsed (json/read-str input-string)
        posted-time (get-in parsed ["postedTime"])
        year-month-day (.substring posted-time 0 10)
        urls (map #(get % "expanded_url") (get-in parsed ["gnip" "urls"])) ]
  {"tweetId" (get parsed "id")
   "postedTime" posted-time
   "postedDate" year-month-day
   "body" (get parsed "body")
   "urls" urls}))

(defn run
  "Run the stream ingestion.
  This pushes events onto two lists:
   - 'input-queue' - a queue for processing
   - 'input-log-YYYY-MM-DD' - the log of inputs. This is written to a log file.
  Blocks forever."
  []
  (let [^Jedis redis-conn (new Jedis (:redis-host env) (Integer/parseInt (:redis-port env)))
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