(ns event-data-twitter-agent.process
  "Process input queue."
  (:require [config.core :refer [env]])
  (:require [event-data-twitter-agent.util :as util])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest])
  (:import [redis.clients.jedis Jedis])
  (:import [java.util UUID])
  (:gen-class))

(defn query-reverse-api
  "Query the reverse API with a URL or a snippet of text."
  [query]
  (let [response @(http/get (:reverse-api-url env) {:query-params {"q" query}})]
    (when (= 200 (:status response))
      (:body response))))

(defn extract-dois
  "Accept a hashmap of the kind generated in event-data-twitter-agent.stream and return a seq of DOIs."
  [item]
  ; For now, none. Automatic failure.
  (let [link-dois (map query-reverse-api (get item "urls"))
        doi-from-body (query-reverse-api (get item "body"))
        all-dois (distinct (remove nil? (cons doi-from-body link-dois)))]
      (l/info "Extract from" item "got" (doall all-dois))
    all-dois))

(defn- new-uuid []
  (.toString (UUID/randomUUID)))

(defn run
  "Run and block, processing input queue.
  This can safely run in a few processes concurrently."
  []
  (let [^Jedis redis-conn (util/jedis-connection)]
    (loop []
      ; See http://redis.io/commands/rpoplpush for reliable queue pattern.
      (let [item-str (.brpoplpush redis-conn "input-queue" "input-put-queue-working" 0)
            item-parsed (json/read-str item-str)
            ; seq of DOIs found.
            results (extract-dois item-parsed)

            results-struct (map #(hash-map "doi" % "event-id" (new-uuid)) results)

            ; Wrap the whole thing up.
            serialized-processed-item (json/write-str {"events" results-struct
                                                       "input" item-parsed})]

        (if (empty? results)
            ; If there were no DOI matches, write this to the unmatched log for the posted date.
            (.rpush redis-conn (str "unmatched-log-" (get item-parsed "postedDate")) (into-array [serialized-processed-item]))

            ; If there were matches, write to the matched log plus queue for pushing.
            (do
              (.lpush redis-conn "push-queue" (into-array [serialized-processed-item]))
              (.rpush redis-conn (str "matched-log-" (get item-parsed "postedDate")) (into-array [serialized-processed-item]))))


        ; Once this is done successfully remove from teh working queue.
        (.lrem redis-conn "input-put-queue-working" 0 item-str))
      (recur))))
