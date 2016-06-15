# Event Data Twitter Agent

Agent for collecting event data from Gnip's Twitter data source and pushing into Event Data.

Depends on Gnip service and S3 for storage. 

Work-in-progress as of June 2016.

## S3 Structure

Archives data in an S3 bucket.

 - `«s3-bucket»/rules/current.json` - current set of Gnip rules in play. Rules saved as a JSON array of Gnip rules.
 - `«s3-bucket»/rules/«iso-8601-date».json` - all rule updates. Rules saved as a JSON array of Gnip rules.
 - `«s3-bucket»/logs/«YYYY-MM-DD»/input.json` - input events

## Config

See `src/event_data_twitter_agent/main.clj`, `config-keys` for all the required config values. These can be supplied in `config/dev/config.edn`, `config/prod/config.edn` or as environment variables.

## Running

### Update rules

To update the rules with the latest set of domains, once a month or so. This will fetch member domains and prefixes from the DOI Destinations service.

    lein with-profile dev run update-rules

### Ingest

To inget data from twitter and enqueue. This should run all the time.
    
    lein with-profile dev run ingest

### Process

To process tweets to extact events from them. This should be run all the time. This can be run in several parallel processes for load distribution.

    lein with-profile dev run process

### Daily cron job

Daily jobs. Upload logs.

    lein with-profile dev run daily

## Plumbing

Redis is used for short-term storage. Every day logs are flushed out to S3 storage. Lists named `queue` are pushed and popped and used as a queue, which should be remain a sensible size. Lists named `log` are accumulated over the course of a day and then uploaded to S3 then deleted.


## Deploy

Cron:

    # daily log rotation
    0 5 * * * cd /home/deploy/event-data-twitter-agent && lein with-profile prod run daily

    # monthly rule update
    0 0 1 * * cd /home/deploy/event-data-twitter-agent && lein with-profile prod run update-rules

Supervisor or other runner

    lein with-profile dev run ingest
    lein with-profile dev run process

Run 2 or more process instances.

This is the pipeline:

 - inputs from Gnip -> `input-queue` and -> `input-log-YYYY-MM-DD`
 - `input-queue` -> processing to extract DOIs -> `matched-queue` and `matched-log-YYYY-MM-DD` on success, `unmatched-log-YYYY-MM-DD.
 - `matched-queue` -> Lagotto upload
 - `input-log-YYYY-MM-DD`, `matched-log-YYYY-MM-DD`, `unmatched-log-YYYY-MM-DD` uploaded to S3 every day then cleared.


## License

Copyright © 2016 Crossref

Distributed under the MIT License.
