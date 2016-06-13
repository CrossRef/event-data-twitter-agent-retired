(ns event-data-twitter-agent.util
  "Utility functions"
  (:require [config.core :refer [env]])
  (:require [clojure.tools.logging :as l])
  (:import [com.amazonaws.services.s3 AmazonS3 AmazonS3Client]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.s3.model GetObjectRequest PutObjectRequest])
  (:import [redis.clients.jedis Jedis])
  (:import [java.util UUID])
  (:gen-class))


(defn jedis-connection
  []
  (let [^Jedis redis-conn (new Jedis (:redis-host env) (Integer/parseInt (:redis-port env)))]
    (.select redis-conn (Integer/parseInt (:redis-db-number env)))
    redis-conn))

(defn aws-client
  []
  (new AmazonS3Client (new BasicAWSCredentials (:s3-access-key-id env) (:s3-secret-access-key env))))