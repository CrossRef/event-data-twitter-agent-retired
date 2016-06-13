(ns event-data-twitter-agent.persist
  "Persist things in storage (S3)."
  (:require [config.core :refer [env]])
  (:require [clojure.tools.logging :as l])
  (:require [org.httpkit.client :as http])
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:require [robert.bruce :refer [try-try-again]])
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

(defn upload
  "Upload a file, return true if it worked."
  [local-file remote-name]
  (l/info "Uploading" local-file "to" remote-name)
  (let [^AmazonS3 client (new AmazonS3Client)
        request (new PutObjectRequest (:archive-s3-bucket env) remote-name local-file)
        put-result (.putObject client request)]
    
    ; S3 isn't transactional, may take a while to propagate. Try a few times to see if it uploaded OK, return success.
    (try-try-again {:sleep 5000 :tries 10 :return? :truthy?} (fn []
      (.doesObjectExist client  (:archive-s3-bucket env) remote-name)))))

(defn stash-list
  "Stash a list of strings into a file in S3 and remove the key, if the key exists.
  In practice this will be a list of JSON objects."
  [list-name remote-name]
  (l/info "Attempt stash" list-name " -> " remote-name)
  (let [tempfile (java.io.File/createTempFile "event-data-twitter-agent-stash" nil)
        ^Jedis redis-conn (new Jedis (:redis-host env) (Integer/parseInt (:redis-port env)))
        list-range (.lrange redis-conn list-name 0 -1)
        counter (atom 0)
        key-exists (.exists redis-conn list-name)]
    
    (if-not key-exists
      (l/info "Key" list-name "did not exist. This is expected for anything older than yesterday.")
      (do 
        (l/info "Key" list-name "found")
      
        ; Write each line in the list separated by newline
        (with-open [writer (io/writer tempfile)]
          (doseq [line list-range]
            (swap! counter inc)
            (.write writer line)
            (.write writer "\n")))
        (l/info "Saved " @counter "lines to" tempfile)

        ; If the upload worked OK, delete from Redis.
        (if-not (upload tempfile remote-name)
          (l/error "Failed to upload to " remote-name "!")
          (do
            (l/info "Successful upload, delete list from Redis " list-name)
            (.del redis-conn (into-array [list-name]))))))
    (.delete tempfile)))
