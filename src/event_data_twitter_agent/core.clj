(ns event-data-twitter-agent.core
  (:require [event-data-twitter-agent.rules :as rules]
            [event-data-twitter-agent.stream :as stream]
            [event-data-twitter-agent.process :as process])
  (:require [config.core :refer [env]])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:gen-class))

(def config-keys
  #{
    :member-domains-url ; URL where we can find list of member domains.
    :member-prefixes-url ; URL where we can find the list of member prefixes.
    :archive-s3-bucket ; S3 Bucket name that holds archive information.
    :s3-access-key-id ; AWS access key for putting logs.
    :s3-secret-access-key ; AWS access key for putting logs.
    :gnip-rules-url ; URL Endpoint for fetching and updating rules.
    :gnip-username ; Username for Gnip API Access.
    :gnip-password ; Password for Gnip API Access.
    :redis-host ; host running redis server
    :redis-port ; port of redis server
    :reverse-api-url ; URL of DOI reversal URL
  })

(defn missing-config-keys
  "Ensure all config keys are present. Return missing keys or nil if OK."
  []
  (let [missing (set/difference config-keys (set (keys env)))]
    (when-not (empty? missing)
      missing)))

(defn update-rules
  "Update the Gnip rules. Sends new rules to Gnip and archives."
  []
  (rules/update-all))

(defn run-ingest
  "Run the stream ingestion client."
  []
  (stream/run))

(defn run-process
  "Run the tweet processing."
  []
  (process/run))

(defn invalid-command
  "An invalid command was given"
  [command]
  (l/fatal "An invalid command '" command "' was given. Exiting.")
  (System/exit 1))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (l/info "Starting Event Data Twitter Agent")

  (when-let [missing (missing-config-keys)]
    (l/fatal "Missing keys" missing)
    (System/exit 1))

  (l/info "Started Event Data Twitter Agent")

  (condp = (first args)
    "update-rules" (update-rules)
    "ingest" (run-ingest)
    "process" (run-process)
    (invalid-command (first args))))
