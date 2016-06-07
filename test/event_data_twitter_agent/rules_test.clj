(ns event-data-twitter-agent.rules-test
  (:require [clojure.test :refer :all]
            [event-data-twitter-agent.rules :as rules]))

(deftest create-rule
  (testing "Correct GNIP rule generated from full domain and prefix."
    (is (= "url_contains:\"//www.example.com/\"" (#'rules/create-rule-from-domain "www.example.com")))
    (is (= "contains:\"10.5555/\"" (#'rules/create-rule-from-prefix "10.5555")))))

(deftest format-gnip-ruleset
  (testing "A seq of Gnip rules can be correctly rendered into JSON."
    (is (= "{\"rules\":[{\"value\":\"rule-a\"},{\"value\":\"rule-b\"},{\"value\":\"rule-c\"}]}"
           (#'rules/format-gnip-ruleset ["rule-a" "rule-b" "rule-c"])))))

(deftest parse-gnip-ruleset
  (testing "A Gnip rule JSON set can be correctly parsed."
    (is (= (#'rules/format-gnip-ruleset ["rule-a" "rule-b" "rule-c"])
           "{\"rules\":[{\"value\":\"rule-a\"},{\"value\":\"rule-b\"},{\"value\":\"rule-c\"}]}"))))