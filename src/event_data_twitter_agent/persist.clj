(ns event-data-twitter-agent.persist
  "Persist things in storage (S3)."
  (:require [event-data-twitter-agent.util :as util])
  (:require [config.core :refer [env]])
  (:require [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [com.twitter.hbc.httpclient.auth BasicAuth]
           [com.twitter.hbc ClientBuilder]
           [com.twitter.hbc.core Constants]
           [com.twitter.hbc.core.processor LineStringProcessor]
           [com.twitter.hbc.core.endpoint RealTimeEnterpriseStreamingEndpoint]
           [redis.clients.jedis Jedis])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest])
  (:gen-class))

(defn stash-jsonapi-list
  "Stash a list of JSON-able objects into a file in S3 using JSONAPI format and remove the key from Redis, if the key exists."
  [list-data remote-name json-api-type]
  (let [tempfile (java.io.File/createTempFile "event-data-twitter-agent-stash" nil)
        counter (atom 0)]
    (let [decorated (map #(assoc % "type" json-api-type) list-data)
          api-object {"meta" {"status" "ok" 
                              "type" json-api-type}
                      "data" decorated}]
      (with-open [writer (io/writer tempfile)]
        (json/write api-object writer)))
    
    (l/info "Saved " @counter "lines to" tempfile)

    ; If the upload worked OK, delete from Redis.
    (if-not (util/upload-file tempfile remote-name "application/json")
      (do
        (l/error "Failed to upload to " remote-name "!")
        (.delete tempfile)
        false)
      (do
        (l/info "Successful upload")
        (.delete tempfile)
        true))))

(defn stash-jsonapi-redis-list
  "Get a named Redis list containing JSON strings.
   Stash into a file in S3 using JSONAPI format and remove the key from Redis, if the key exists."
  [list-name remote-name json-api-type]
  (l/info "Attempt stash" list-name " -> " remote-name)
  (let [^Jedis redis-conn (util/jedis-connection)
        list-range (.lrange redis-conn list-name 0 -1)
        key-exists (.exists redis-conn list-name)]
    (if-not key-exists
      (l/info "Key" list-name "did not exist. This is expected for anything older than yesterday.")  
      (let [parsed-list (map json/read-str list-range)
            success (stash-jsonapi-list parsed-list)]
        (when success
          (.del redis-conn (into-array [list-name])))))))

(defn jsonlist-to-jsonapi
  "Helper function to convert legacy jsonlist files to jsonapi compatible files."
  [filename-in filename-out json-api-type]
  (with-open [of (io/writer filename-out)]
    (with-open [inf (io/reader filename-in)]
      (let [inputs (line-seq inf)
            parsed-list (map json/read-str inputs)
            decorated (map #(assoc % "type" json-api-type) parsed-list)
            api-object {"meta" {"status" "ok" 
                                "type" json-api-type}
                        "data" decorated}]
        (json/write api-object of)))))
