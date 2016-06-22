(ns event-data-twitter-agent.push
  (:import  [java.util.logging Logger Level])
  (:require [clojure.tools.logging :as l]
            [clojure.data.json :as json])
  (:require [crossref.util.doi :as cr-doi])
  (:require [baleen.queue :as baleen-queue]
            [baleen.web :as baleen-web]
            [baleen.util :as baleen-util]
            [baleen.lagotto :as baleen-lagotto])
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l]
            [clojure.set :refer [difference]])
  (:require 
            [clj-time.coerce :as clj-time-coerce]))

(defn tweet-id-to-url
  "Transform a tweet id like 'tag:search.twitter.com,2005:740545495802728448' into a URL"
  [tweet-id]
  (let [id (nth (.split tweet-id ":") 2)]
  (str "http://twitter.com/statuses/" id)))

(defn process-f [context json-blob]
  (let [item-parsed (json/read-str json-blob)
        input (get item-parsed "input")
        events (get-in item-parsed ["events"])

        url (get-in item-parsed ["meta" "url"])
          
        results (doall (map (fn [{doi "doi" event-id "event-id"}]
                                  (baleen-lagotto/send-deposit
                                    context
                                    :subj-title (get input "body")
                                    :subj-url (tweet-id-to-url (get input "tweetId"))
                                    :subj-author (get input "author")
                                    :subj-container-title "Twitter"
                                    :subj-work-type "tweet"
                                    :obj-doi doi
                                    :action "add"
                                    :subj-registration-agency "twitter"
                                    :event-id event-id
                                    :date-str (get input "postedTime")
                                    :source-id "twitter"
                                    :relation-type "discusses")) events))]
      ; Return success.
      (every? true? results)))
           

(defn run
  "Run processing. Blocks forever."
  [context]
  (baleen-queue/process-queue context "matched" (partial process-f context) :keep-done true))

