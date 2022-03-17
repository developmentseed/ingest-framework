# Ingest Framework

## Description

A scratchpad for ideas around a data ingestion framework.

## Example

[https://github.com/edkeeble/ingest-example](https://github.com/edkeeble/ingest-example)

## Missing

A non-comprehensive list of things which are missing right now.

- Defining networking requirements (we may be able to infer these from required access to DB in private subnet)
- Support a requirements file per step rather than per app
- Additional trigger types (SQS, HTTP request?)
- Allow triggering another pipeline from within a step (support parallelization)
- Monitoring:
  - tracing each pipeline run through each step in the pipeline
  - an interface for monitoring pipeline runs
  - the ability to retry a run
- Update current approach using class types for steps in a pipeline to a more standard class instance (with parameters passed in constructor)
  - This requires re-instantiating the classes with the same parameters within each lambda function
- A non-toy working example
- A multi-pipeline example
