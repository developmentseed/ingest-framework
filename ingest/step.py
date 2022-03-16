from datetime import timedelta
import json
from typing import List, get_args, Protocol, Sequence, TypeVar

# from uuid import uuid4

from pydantic import UUID4, BaseModel

from ingest.cache import BatchCache

I = TypeVar("I", bound=BaseModel)
O = TypeVar("O", covariant=True, bound=BaseModel)
I_co = TypeVar("I_co", covariant=True)


def get_base(cls):
    """Utility for retrieving the base class"""
    if hasattr(cls, "__orig_bases__"):
        return get_base(cls.__orig_bases__[0])
    else:
        return cls


class Step(Protocol[I_co, O]):
    @classmethod
    def get_output(cls) -> O:
        return get_args(get_base(cls))[1]

    @classmethod
    def get_input(cls) -> I_co:
        return get_args(get_base(cls))[0]

    @classmethod
    def handler(cls, event, context) -> O:
        raise NotImplementedError


class Transformer(Step[I, O]):
    @classmethod
    def execute(cls, input: I) -> O:
        raise NotImplementedError()

    @classmethod
    def handler(cls, event, context) -> O:
        print(f"Event: {event}")
        print(f"Context: {context}")
        input_data = cls.get_input().parse_obj(event)
        print(f"Input: {input_data}")
        result = cls.execute(input=input_data)
        return result


class Collector(Step[I, O]):
    batch_size: int = 100
    max_batching_window: int = 60
    cache: BatchCache

    def collect_input(self, input: I) -> None:
        self.cache.queue_data(data=input)

    def ready(self) -> bool:
        return (
            self.cache.queue_size >= self.batch_size
            or self.cache.time_since_first_item()
            >= timedelta(seconds=self.max_batching_window)
        )

    def fetch_batch(self) -> Sequence[I]:
        return self.cache.fetch(self.batch_size)

    @classmethod
    def execute(cls, input: Sequence[I]) -> O:
        raise NotImplementedError()

    @classmethod
    def handler(cls, event, context) -> O:
        print(event)
        print(context)
        input_type = cls.get_input()
        result = cls.execute(
            input=[
                input_type.parse_obj(json.loads(record.get("body")))
                for record in event["Records"]
            ]
        )
        return result
