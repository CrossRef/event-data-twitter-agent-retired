(ns event-data-twitter-agent.rules
  "Handle Gnip's subscription rules."
  (:require [baleen.stash :as baleen-stash]
            [baleen.context :as baleen-context]
            [baleen.time :as baleen-time]
            
            [baleen.reverse :as baleen-reverse])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json])
  (:import [java.util TimeZone Date]
           [java.text SimpleDateFormat])
  (:gen-class))


(def hardcoded-rules
  "Rules that are hard-coded and aren't be derived from data."
  #{"url_contains:\"doi.org/10.\""})

(def excluded-domains
  "Rules that can never match. Manually curated."
  #{
    "issuu.com" ; This never serves any metadata and isn't really a publishing platform.
  })

(defn- format-gnip-ruleset
  "Format a set of string rules into a JSON object."
  [rule-seq]
  (let [structure {"rules" (map #(hash-map "value" %) rule-seq)}]
    (json/write-str structure)))

(defn- parse-gnip-ruleset
  "Parse the Gnip ruleset into a seq of rule string."
  [json-string]
  (let [structure (json/read-str json-string)
        rules (get structure "rules")]
      (map #(get % "value") rules)))

(defn- fetch-rules-in-play
  "Fetch the current rule set from Gnip."
  [context]
  (let [fetched @(http/get (:gnip-rules-url (baleen-context/get-config context)) {:basic-auth [(:gnip-username (baleen-context/get-config context)) (:gnip-password (baleen-context/get-config context))]})
        rules (-> fetched :body parse-gnip-ruleset)]
    (set rules)))

(defn archive-rules
  "Archive the current list of rules to the log on S3. Save as both 'current' and timestamp."
  [context rules]
  (let [current-keyname "filter-rules/current.json"
        timestamp-keyname (str "filter-rules/" (baleen-time/iso8601-now) ".json")]
    (baleen-stash/stash-jsonapi-list context rules current-keyname "gnip-rule" true)
    (baleen-stash/stash-jsonapi-list context rules timestamp-keyname "gnip-rule" false)))

(defn- create-rule-from-domain
  "Create a Gnip rule from a full domain, e.g. www.xyz.com, if valid or nil."
  [full-domain]
  ; Basic sense check.
  (when (and
      (> (.length full-domain) 3)
      (not (excluded-domains full-domain)))
    (str "url_contains:\"//" full-domain "/\"")))

(defn- create-rule-from-prefix
  "Create a Gnip rule from a DOI prefix, e.g. 10.5555"
  [prefix]
  (str "contains:\"" prefix "/\""))

(defn- add-rules
  "Add rules to Gnip."
  [context rules]
  (let [result @(http/post (:gnip-rules-url (baleen-context/get-config context)) {:body (format-gnip-ruleset rules) :basic-auth [(:gnip-username (baleen-context/get-config context)) (:gnip-password (baleen-context/get-config context))]})]
    (when-not (#{200 201} (:status result))
      (l/fatal "Failed to add rules" result))))

(defn- remove-rules
  "Add rules to Gnip."
  [context rules]
  (let [result @(http/delete (:gnip-rules-url (baleen-context/get-config context)) {:body (format-gnip-ruleset rules) :basic-auth [(:gnip-username (baleen-context/get-config context)) (:gnip-password (baleen-context/get-config context))]})]
    (when-not (#{200 201} (:status result))
      (l/fatal "Failed to delete rules" result))))


(defn update-all
  "Perform complete update cycle of Gnip rules.
  Do this by fetching the list of domains and prefixes from the 'DOI Destinations' service, creating a rule-set then diffing with what's already in Gnip.
  Archive these to S3."
  [context]
  (let [current-rule-set (fetch-rules-in-play)
        new-rules-set (baleen-reverse/fetch-domains context)
        rules-to-add (clojure.set/difference new-rules-set current-rule-set)
        rules-to-remove (clojure.set/difference current-rule-set new-rules-set)
        ; Format for saving in archive.
        new-rules-to-save (map #(hash-map "rule" %) new-rules-set)]
    (l/info "Current rules " (count current-rule-set) ", up to date rules " (count new-rules-set))
    (l/info "Add" (count rules-to-add) ", remove " (count rules-to-remove))

    (archive-rules context new-rules-to-save)
    (add-rules context rules-to-add)
    (remove-rules context rules-to-remove)

    ; Now re-fetch to see if we got the desired result.
    (let [current-rule-set-now (fetch-rules-in-play)]
      (when-not (= current-rule-set-now new-rules-set)
        (l/fatal "Rule set update not reflected!")))))