# Event Data Twitter Agent

Agent for collecting event data from Gnip's Twitter data source and pushing into Event Data.

Depends on Gnip service and S3 for storage. 

## S3 Structure

Archives data in an S3 bucket.

 - `«s3-bucket»/rules/current.json` - current set of Gnip rules in play. Rules saved as a JSON array of Gnip rules.
 - `«s3-bucket»/rules/«iso-8601-date».json` - all rule updates. Rules saved as a JSON array of Gnip rules.

## Config

See `src/event_data_twitter_agent/main.clj`, `config-keys` for all the required config values. These can be supplied in `config/dev/config.edn`, `config/prod/config.edn` or as environment variables.

## Running

### Update rules

To update the rules with the latest set of domains, once a month or so. This will fetch member domains and prefixes from the DOI Destinations service.

    lein with-profile dev run update-rules

### Ingest

To inget data from twitter and enqueue. This should run all the time.
    
    lein with-profile dev run ingest


## License

Copyright © 2016 Crossref

Distributed under the MIT License.
