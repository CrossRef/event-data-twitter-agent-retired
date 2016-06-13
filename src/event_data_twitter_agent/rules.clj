(ns event-data-twitter-agent.rules
  "Handle Gnip's subscription rules."
  (:require [config.core :refer [env]])
  (:require [clojure.set :as set]
            [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest])
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

(defn- utc-timestamp
  []
  (let [date-format (new SimpleDateFormat "yyyy-MM-dd'T'HH:mm'Z'")]
        (.setTimeZone date-format (TimeZone/getTimeZone "UTC"))
    (.format date-format (new Date))))

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
  []
  (let [fetched @(http/get (:gnip-rules-url env) {:basic-auth [(:gnip-username env) (:gnip-password env)]})
        rules (-> fetched :body parse-gnip-ruleset)]
    (set rules)))

(defn archive-rules
  "Archive the current list of rules to the log on S3. Save as both 'current' and timestamp."
  [rules]
  (let [^AmazonS3 client (new AmazonS3Client (new BasicAWSCredentials (:s3-access-key-id env) (:s3-secret-access-key env)))
        current-keyname "filter-rules/current.json"
        timestamp-keyname (str "filter-rules/" (utc-timestamp) ".json")

        tempfile (java.io.File/createTempFile "event-data-twitter-agent" nil)
        rules-json (json/write-str rules)]

        (spit tempfile rules-json)

        (.putObject client (new PutObjectRequest (:archive-s3-bucket env) current-keyname tempfile))
        (.putObject client (new PutObjectRequest (:archive-s3-bucket env) timestamp-keyname tempfile))

        (.delete tempfile)))

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

(defn- fetch-up-to-date-rules
  "Fetch a new set of rules from the DOI Destinations service."
  []
  (let [domain-rules (->> env :member-domains-url http/get deref :body json/read-str (keep create-rule-from-domain) set)
        prefix-rules (->> env :member-prefixes-url http/get deref :body json/read-str (keep create-rule-from-prefix) set)]
    (clojure.set/union hardcoded-rules domain-rules prefix-rules)))

(defn- add-rules
  "Add rules to Gnip."
  [rules]
  (let [result @(http/post (:gnip-rules-url env) {:body (format-gnip-ruleset rules) :basic-auth [(:gnip-username env) (:gnip-password env)]})]
    (when-not (#{200 201} (:status result))
      (l/fatal "Failed to add rules" result))))

(defn- remove-rules
  "Add rules to Gnip."
  [rules]
  (let [result @(http/delete (:gnip-rules-url env) {:body (format-gnip-ruleset rules) :basic-auth [(:gnip-username env) (:gnip-password env)]})]
    (when-not (#{200 201} (:status result))
      (l/fatal "Failed to delete rules" result))))


(defn update-all
  "Perform complete update cycle of Gnip rules.
  Do this by fetching the list of domains and prefixes from the 'DOI Destinations' service, creating a rule-set then diffing with what's already in Gnip.
  Archive these to S3."
  []
  (let [current-rule-set (fetch-rules-in-play)
        new-rules-set (fetch-up-to-date-rules)
        rules-to-add (clojure.set/difference new-rules-set current-rule-set)
        rules-to-remove (clojure.set/difference current-rule-set new-rules-set)]
    (l/info "Current rules " (count current-rule-set) ", up to date rules " (count new-rules-set))
    (l/info "Add" (count rules-to-add) ", remove " (count rules-to-remove))

    (archive-rules new-rules-set)
    (add-rules rules-to-add)
    (remove-rules rules-to-remove)

    ; Now re-fetch to see if we got the desired result.
    (let [current-rule-set-now (fetch-rules-in-play)]
      (when-not (= current-rule-set-now new-rules-set)
        (l/fatal "Rule set update not reflected!")))))