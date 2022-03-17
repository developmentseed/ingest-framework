# Ingest Framework

## Description

A scratchpad for ideas around a data ingestion framework.

## Example

[https://github.com/edkeeble/ingest-example](https://github.com/edkeeble/ingest-example)

## Missing

A non-comprehensive list of things which are obviously missing right now.

- Defining required permissions for each step (S3 bucket access, etc)
- Defining networking requirements (we may be able to infer these from required access to DB in private subnet)
- Additional trigger types (SQS, HTTP request?)
- Monitoring:
  - tracing each pipeline run through each step in the pipeline
  - an interface for monitoring pipeline runs
  - the ability to retry a run
- A non-toy working example
- A multi-pipeline example
