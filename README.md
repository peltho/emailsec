## Email Fraud Detection Service


### How does it work?
1. **Ingestion service** - Ingest all tenant's emails
* Expect a HTTP POST call to `/api/v1/emails/ingest` with `tenant_id` as body
* Normalize emails (may differ per provider)
* Store normalized email batches (of 500 by default) into DB
* Notify broker about processed batches

2. **Fraud detection service** - Detect potential frauds based on pre-defined rules
* Expect to receive broker messages with email ID list (500 max by default)
* Flag email based on severity level
* Send warnings when severity is exceeding a given threshold 

Both services are independant and can be ran at different times. They rely on an external broker to send / receive events.

Ingestion service is expecting a direct call to ingest a tenant ID.

Fraud detection service is a consumer-based (asynchronous) service waiting for events to analyze emails.

### How to run it?

There's a Makefile allowing you to run the services with `make` command.

* `make build` to build both services
* `make run` to run them
* `make test` to run unit tests
* `make integration-test` to run integration tests
* `make help` for all available commands

Everything will run in Docker containers. Integration tests are running ephemeral docker containers (for testing DB) too.

### Fraud detection features (not implemented yet)

What to implement ideally:

* One can compare `from` and `reply-to` fields if they differ
* Check links and buzz words like `urgent`, `pay`, `payment`, `transfer`, `€`, `$`, etc.
* Analyze attachments to be sure they are not malicious
* Get DKIM signature from email header and check its validity (DomainKeys Identified Mail)
* Count occurrences of all senders from tenant emails - if it's a new sender, watch out.
* Use a dedicated tool with IA or an existing machine-learning system

Based on these factors compute a score of risk.
I'd suggest to weight each check differently and compute a Gaussian similarity score based on it.