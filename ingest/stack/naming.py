from ingest.pipeline import Pipeline
from ingest.step import Collector


def collector_queue_name(pipeline: Pipeline, collector: Collector):
    # this needs some protection against reusing
    # the same collector class within one pipeline
    return f"{pipeline.resource_name}_{collector.name}_queue"
