## Email Fraud Detection Service


### How does it work?
1. **Ingestion service** - Ingest all tenant's emails
* Expect a HTTP POST call to `/api/v1/emails/ingest` with `tenant_id` as body
* Normalize emails (may differ per provider)
* Store normalized email batches into DB
* Notify broker about processed batches

2. **Fraud detection service** - Detect potential frauds based on pre-defined rules
* Flag email based on severity level
* Send warnings when severity is exceeding a given threshold 

Both services are independant and can be ran at different times. They rely on an external broker to send / receive events.

Ingestion service is expecting a direct call to ingest a tenant info.

Fraud detection service is a consumer-based (asynchronous) service waiting for events to handle.