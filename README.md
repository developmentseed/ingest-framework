# Ingest Framework

## Description

A framework for developing data ingestion pipelines and deploying them in a serverless fashion.

## Why build a new framework?

At Development Seed, we build a lot of data pipelines, and we've settled on a reference implementation for the majority of cases. We could make use of an existing ETL tool to build out our applications, but we have a fairly limited set of requirements, as well as some odd constraints (e.g. limited permissions in specific client environments), so it makes sense to build a lightweight framework that specifically targets our needs. We have the talent and capacity to extend it, as needed, and we can govern it in such a way that it stays focused on our specific needs. This doesn't need to be a one-size-fits-all solution that eventually collapses under the weight of 1000 edge cases. If it works well for 80% of our pipeline projects, that's good enough.

## Goals

Having worked on data ingestion for the majority of my time here, I've experienced first-hand some of the pain points in developing, testing and monitoring a pipeline. With that in mind, here are my goals for this project:

### Decouple business logic from infrastructure

Just because the code ends up deployed in a Lambda doesn't mean it needs to look like a Lambda handler. We should be able to write our business logic in a way that allows it to be run anywhere, if only because it makes testing easier.

### Provide an easy way to reason about all steps in a pipeline and how they interact

I should be able to look at the definition of a pipeline and reason, at a glance, about how data moves through it. I shouldn't need to jump between CDK constructs and application code. We should also be able to enforce a contract between each step in a pipeline and the subsequent step. We shouldn't have to deploy the pipeline to discover that one step is passing the wrong data type to the next step.

### Provide the ability to run and test an entire pipeline locally

I shouldn't need to deploy my pipeline and all of its dependencies (VPC, Database, etc) in AWS in order to run an end-to-end test. I should be able to spin up any required services locally and run my entire pipeline from my dev machine.

### Provide an out-of-the-box monitoring interface for every pipeline

Monitoring is a pain to add after the fact and it's especially a pain to add to an adhoc pipeline. We should bake it into the framework, so it's available for free with every deployment. The monitoring tool should represent the logical pipelines defined in your application, decoupled from the infrastructure that runs them. So, even if a pipeline consists of two or more Step Functions behind the scenes, it appears as a single pipeline in the monitoring interface.

### Codify best practices in code

We now have a reference design for this sort of ingestion pipeline. But the only thing better than codifying best practices in a document is codifying them in, well, code. For example, we shouldn't have to consult the reference doc every time we want to implement a bulk processing step. We should be able to simply annotate a step in our pipeline as accumulating data from other pipeline runs and let the framework handle provisioning all the required resources to make that happen.

## Example

[https://github.com/edkeeble/ingest-example](https://github.com/edkeeble/ingest-example)

## Missing

A non-comprehensive list of things which are missing right now.

- [ ] Defining networking requirements (we may be able to infer these from required access to DB in private subnet)
- [x] Support a requirements file per step rather than per app
- [ ] Additional trigger types (SQS, HTTP request?). Define a plugin interface for triggers, so that other devs can provide their own.
- [ ] Support injecting named secrets into a step
- [ ] Allow triggering another pipeline from within a step (support parallelization)
- [ ] Monitoring:
  - [ ] tracing each pipeline run through each step in the pipeline
  - [ ] an interface for monitoring pipeline runs
  - [ ] the ability to retry a run
- [ ] Update current approach using class types for steps in a pipeline to a more standard class instance (with parameters passed in constructor)
  - This requires re-instantiating the classes with the same parameters within each lambda function
- [ ] A non-toy working example
- [ ] A multi-pipeline example
- [ ] Move the examples into this repo
- [ ] A better name for this project
