(ns event-data-twitter-agent.core
  (:require [event-data-twitter-agent.rules :as rules]
            [event-data-twitter-agent.stream :as stream]
            [event-data-twitter-agent.process :as process]
            [event-data-twitter-agent.push :as push])
  (:require [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [baleen.context :as baleen]
            [baleen.monitor :as baleen-monitor]
            [baleen.stash :as baleen-stash])
  (:gen-class))

(def config-keys
  #{
    :gnip-rules-url ; URL Endpoint for fetching and updating rules.
    :gnip-username ; Username for Gnip API Access.
    :gnip-password ; Password for Gnip API Access.
  })

(defn update-rules
  "Update the Gnip rules. Sends new rules to Gnip and archives."
  [context]
  (rules/update-all context))

(defn run-ingest
  "Run the stream ingestion client. Runs forever."
  [context ]
  (baleen-monitor/register-heartbeat context "ingest")
  (stream/run context))

(defn run-process
  "Run the tweet processing. Runs forever, more than one can be run at once."
  [context]
  (baleen-monitor/register-heartbeat context "process")
  (process/run context))

(defn run-push
  "Run the event pushing. Runs forever."
  [context]
  (baleen-monitor/register-heartbeat context "push")
  (push/run context))

(defn run-monitor
  [context]
  (l/info "Monitor")
  ; Monitor, monitor thyself.
  (baleen-monitor/register-heartbeat context "monitor")
  (baleen-monitor/run context))

(def ymd (clj-time-format/formatter "yyyy-MM-dd"))

(defn run-daily
  "Run daily tasks. Stashing logs. This will run the last 30 days' worth of daily tasks (starting with yesterday) if they haven't been done."
  [context]
  (let [interval-range (map #(clj-time/minus (clj-time/now) (clj-time/days %)) (range 1 30))
        ; as strings of YYYY-MM-DD
        ymd-range (map #(clj-time-format/unparse ymd %) interval-range)]
    (l/info "Checking " (count ymd-range) "past days")
    (doseq [date-str ymd-range]
      (l/info "Check " date-str)
      (baleen-stash/stash-jsonapi-redis-list context (str "input-" date-str) (str "logs/" date-str "/input.json") "twitter-input" false)
      (baleen-stash/stash-jsonapi-redis-list context (str "matched-" date-str) (str "logs/" date-str "/matched.json") "twitter-match" false)
      (baleen-stash/stash-jsonapi-redis-list context (str "unmatched-" date-str) (str "logs/" date-str "/unmatched.json") "twitter-match" false))))

(defn invalid-command
  "An invalid command was given"
  [command]
  (l/fatal "An invalid command '" command "' was given. Exiting.")
  (System/exit 1))

(defn -main
  [& args]

  (let [context (baleen/->Context
                  "twitter"
                  "Twitter Event Data Agent"
                  config-keys)

        command (first args)]

    (when-not (baleen/boot! context)
      (System/exit 1))

    (l/info "Command: " command)

  (condp = (first args)
    "update-rules" (update-rules context)
    "ingest" (run-ingest context)
    "process" (run-process context)
    "push" (run-push context)
    "daily" (run-daily context)
    "monitor" (run-monitor context)
    (invalid-command (first args)))))
