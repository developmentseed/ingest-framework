from abc import ABC
from datetime import timedelta
from typing import Dict, Generic, get_args, Protocol, Sequence, TypeVar
from uuid import uuid4

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


class Step(ABC, Generic[I_co, O]):
    uuid: UUID4
    # name: str
    parameters: Dict

    def __init__(self, *args, **kwargs):
        if type(self) == Step:
            raise Exception("Step must be subclassed")
        # self.name = name
        self.uuid = uuid4()
        self.parameters = kwargs

    # @classmethod
    # def __new__(cls, *args, **kwargs):
    #     if cls is Step:
    #         raise TypeError(f"only children of '{cls.__name__}' may be instantiated")
    #     return object.__new__(cls, *args, **kwargs)

    @classmethod
    def get_output(cls) -> O:
        return get_args(get_base(cls))[1]

    @classmethod
    def get_input(cls) -> I_co:
        return get_args(get_base(cls))[0]

    @classmethod
    def handler(cls, raw_event, context) -> O:
        raise NotImplementedError


class Transformer(Step[I, O]):
    @classmethod
    def execute(cls, input: I, *args, **kwargs) -> O:
        raise NotImplementedError()

    @classmethod
    def handler(cls, raw_event, context) -> O:
        print(f"Event: {raw_event}")
        print(f"Context: {context}")
        input_data = cls.get_input().parse_obj(raw_event)
        print(f"Input: {input_data}")
        result = cls.execute(input=input_data)
        return result
        # pass result on to next step


# TODO: Refactor to make all methods static and accept pipeline/step references for caching
class Collector(Step[I, O]):
    batch_size: int = 100
    max_batching_window: int = 60
    cache: BatchCache

    # def __init__(self, batch_size: int = 100, max_batching_window: int = 60, **kwargs):
    #     super().__init__(
    #         # name=name,
    #         batch_size=batch_size,
    #         max_batching_window=max_batching_window,
    #         **kwargs
    #     )
    #     self.batch_size = batch_size
    #     self.max_batching_window = max_batching_window
    #     self.cache = BatchCache()

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
    def execute(cls, input: Sequence[I], *args, **kwargs) -> O:
        raise NotImplementedError()

    @classmethod
    def handler(cls, raw_event, context) -> O:
        print(raw_event)
        print(context)
        input = raw_event
        result = cls.execute(input)
        return result
        # pass result on to next step
