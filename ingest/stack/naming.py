from typing import Type
from ingest.step import Collector


def collector_queue_name(collector: Type[Collector]):
    # this needs some protection against reusing
    # the same collector class within one pipeline
    return f"{collector.__name__}_queue"
