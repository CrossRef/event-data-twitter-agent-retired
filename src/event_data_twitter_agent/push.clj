(ns event-data-twitter-agent.push
  (:require [event-data-twitter-agent.util :as util])
  (:import  [java.util.logging Logger Level])
  (:require [clojure.tools.logging :as l]
            [clojure.data.json :as json])
  (:require [org.httpkit.client :as http-client]
            [config.core :refer [env]]
            [crossref.util.doi :as cr-doi])
  (:import [redis.clients.jedis Jedis])

  )

(defn send-deposit
  [&{:keys [subj-title subj-url subj-author subj-container-title subj-work-type obj-doi action subj-registration-agency event-id date-str source-id relation-type]}]
  (let [endpoint (env :lagotto-push-endpoint)
        source-token (env :lagotto-source-token)
        auth-token (env :lagotto-auth-token)

        subject_metadata {"pid" subj-url
                          "author" {"literal" subj-author}
                          "title" subj-container-title
                          "container-title" subj-container-title
                          "issued" date-str
                          "URL" subj-url
                          "type" subj-work-type
                          "tracked" false
                          "registration_agency_id" subj-registration-agency }

        payload (condp = action
          ; No message action when adding, only deleting.
          "add" {:deposit {:uuid event-id
                           :source_token source-token
                           :subj_id subj-url
                           :obj_id (cr-doi/normalise-doi obj-doi)
                           :relation_type_id relation-type
                           :source_id source-id
                           :subj subject_metadata}}
          "remove" {:deposit {:uuid event-id
                              :message_action "delete"
                              :source_token source-token
                              :subj_id subj-url
                              :obj_id (cr-doi/normalise-doi obj-doi)
                              :relation_type_id relation-type
                              :source_id source-id
                              :subj subject_metadata}}
          nil)]
        (l/info "Sending payload" payload)
    (when payload
      (let [result @(http-client/request 
                   {:url endpoint
                    :method :post
                    :headers {"Authorization" (str "Token token=" auth-token) "Content-Type" "application/json"}
                    :body (json/write-str payload)})]
      (l/info "Result:" result)
      (= (:status result) 202)))))

(defn tweet-id-to-url
  "Transform a tweet id like 'tag:search.twitter.com,2005:740545495802728448' into a URL"
  [tweet-id]
  (let [id (nth (.split tweet-id ":") 2)]
  (str "http://twitter.com/statuses/" id)))

(defn run
  "Run and block, processing input queue.
  This can safely run in a few processes concurrently."
  []
  (l/info "Start pusher")
  (let [^Jedis redis-conn (util/jedis-connection)]
    (loop []
      (let [item-str (.brpoplpush redis-conn "push-queue" "push-queue-working" 0)
            item-parsed (json/read-str item-str)
            
            ; seq of events (doi and event-id)
            events (get item-parsed "events")

            ; the tweet data.
            input (get item-parsed "input")

            push-results (doall (map (fn [{doi "doi"
                                    event-id "event-id"}]
                                (prn "push" doi event-id)
                                (prn "input" input)

                                  (send-deposit :subj-title (get input "body")
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
                                        :relation-type "discusses")
                                    ) events))
            ]
          (l/info "Push queue got" item-str)
          (l/info "Push results" push-results)

          ; If it failed, leave input on `push-queue-working` for investigation.
          ; If it worked, remove it but also put on `push-queue-done`, just in case.
          (when (every? true? push-results)
            (.lpush redis-conn "push-queue-done" (into-array [item-str]))
            (.lrem redis-conn "push-queue-working" 0 item-str)))
      (recur))))
