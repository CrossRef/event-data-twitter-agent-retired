(ns event-data-twitter-agent.process
  "Process input queue."
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest])
  (:import [redis.clients.jedis Jedis])
  (:import [java.util UUID])
  (:require [baleen.util :as baleen-util]
            [baleen.queue :as baleen-queue]
            [baleen.reverse :as baleen-reverse])
  (:gen-class))

(defn extract-dois
  "Accept a hashmap of the kind generated in event-data-twitter-agent.stream and return a seq of DOIs."
  [context item]
  ; For now, none. Automatic failure.
  (let [link-dois (map (partial baleen-reverse/query-reverse-api context) (get item "urls"))
        doi-from-body (baleen-reverse/query-reverse-api context (get item "body"))
        all-dois (distinct (remove nil? (cons doi-from-body link-dois)))]
      (l/info "Extract from" item "got" (doall all-dois))
    all-dois))

(defn process-f
  [context json-blob]
  (let [parsed-event (json/read-str json-blob)
        ; seq of DOIs found.
        results (extract-dois context parsed-event)
        results-struct (map #(hash-map "doi" % "event-id" (baleen-util/new-uuid)) results)

       ; Wrap the whole thing up.
       serialized-processed-item (json/write-str {"events" results-struct
                                                   "input" parsed-event})]
    (if (empty? results)
      ; If there were no DOI matches, write this to the unmatched log for the posted date.
      (baleen-queue/enqueue context "unmatched" serialized-processed-item true)

      ; If there were matches, write to the matched log plus queue for pushing.
      (baleen-queue/enqueue context "matched" serialized-processed-item true))))


(defn run
  "Run processing. Blocks forever.
  This can safely run in a few processes concurrently."
  [context]
  (l/info "Run process.")
  (baleen-queue/process-queue context "input" (partial process-f context)))
