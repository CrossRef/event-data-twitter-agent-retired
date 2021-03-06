(ns event-data-twitter-agent.stream
  "Handle Gnip's stream and put into Redis."
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [com.twitter.hbc.httpclient.auth BasicAuth]
           [com.twitter.hbc ClientBuilder]
           [com.twitter.hbc.core Constants]
           [com.twitter.hbc.core.processor LineStringProcessor]
           [com.twitter.hbc.core.endpoint RealTimeEnterpriseStreamingEndpoint])
  (:require [clj-time.coerce :as clj-time-coerce])
  (:require [baleen.context :as baleen-context]
            [baleen.queue :as baleen-queue])
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
        urls (map #(get % "expanded_url") (get-in parsed ["gnip" "urls"]))
        matching-rules (map #(get % "value") (get-in parsed ["gnip" "matching_rules"]))]
  {"tweetId" (get parsed "id")
   "author" (get-in parsed ["actor" "link"])
   "postedTime" posted-time
   "body" (get parsed "body")
   "urls" urls
   "matchingRules" matching-rules}))

(defn run
  "Run the stream ingestion.
  This pushes events onto two lists:
   - 'input-queue' - a queue for processing
   - 'input-log-YYYY-MM-DD' - the log of inputs. This is written to a log file.
  Blocks forever."
  [context]
  (let [q (new LinkedBlockingQueue 1000) 
        client (-> (new ClientBuilder)
                   (.hosts Constants/ENTERPRISE_STREAM_HOST)
                   (.endpoint (new RealTimeEnterpriseStreamingEndpoint "Crossref" "track" "prod"))
                   (.authentication (new BasicAuth (:gnip-username (baleen-context/get-config context)) (:gnip-password (baleen-context/get-config context))))
                   (.processor (new com.twitter.hbc.core.processor.LineStringProcessor q))
                   (.build))]
        (l/info "Connecting to Gnip...")
        (.connect client)
        (l/info "Connected to Gnip.")

        (loop []
          ; Block on the take.
          (let [event (.take q)
                parsed (parse-entry event)
                posted-date (clj-time-coerce/from-string (get parsed "postedTime"))
                serialized (json/write-str parsed)]
            ; Parsed does actually transform input, we're not just parsing and unparsing for the sake of it.
            (baleen-queue/enqueue-with-time context "input" posted-date serialized true))
          (recur))))
