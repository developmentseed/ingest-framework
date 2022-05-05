from dataclasses import dataclass, field
from datetime import timedelta
import json
from inspect import signature
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Mapping,
    Sequence,
    TypeVar,
    List,
    get_args,
)

from pydantic import UUID4, BaseModel

from ingest.permissions import Permission
from ingest.providers import CloudProvider, bootstrap
from ingest.services.queue import IQueue

from kink import inject

I = TypeVar("I", bound=BaseModel)
O = TypeVar("O", covariant=True, bound=BaseModel)
I_co = TypeVar("I_co", covariant=True)


def get_base(cls):
    """Utility for retrieving the base class"""
    if hasattr(cls, "__orig_bases__"):
        return get_base(cls.__orig_bases__[0])
    else:
        return cls


@dataclass
class Step(Generic[I_co, O]):
    func: Callable[[I_co], O]
    aws_lambda_properties: Mapping[str, Any] = field(default_factory=dict)
    env_vars: Sequence[str] = field(default_factory=list)
    permissions: Mapping[CloudProvider, Sequence[Permission]] = field(
        default_factory=dict
    )
    requirements: Sequence[str] = field(default_factory=list)

    def get_output(self) -> O:
        return signature(self.func).return_annotation

    def get_input(self) -> I_co:
        return signature(self.func).parameters["input"].annotation

    def lambda_handler(self, event, context):
        """
        Handler for AWS Lambdas.
        """
        # When running in lambda, we don't instantiate a Pipeline class (where
        # bootstrapping occurs), so we must bootstrap our DI here
        bootstrap(CloudProvider.aws)
        return self(self.get_input().parse_obj(event)).dict(
            by_alias=True, exclude_unset=True
        )

    @property
    def name(self):
        return self.func.__name__


@dataclass
class BasicStep(Step[I, O]):
    def __call__(self, input: I) -> O:
        """
        Inject dependencies and execute step.
        """
        prepped_handler = inject(self.func)
        return prepped_handler(input)


@dataclass
class Collector(Step[List[I], O]):
    """
    A step for processing batches of items.

    When placed in a Pipeline, the previous step will
    post its output to a queue. The Collector step will then
    consume messages off that queue in batches, using
    the configuration below.
    """

    batch_size: int = 100
    max_batching_window: int = 60

    def __call__(self, input: List[I]) -> O:
        """
        Inject dependencies and execute step.
        """
        prepped_handler = inject(self.func)
        return prepped_handler(input)

    def __post_init__(self):
        if not get_args(super().get_input()):
            raise TypeError(
                "`input` parameter for collector steps must be iterable with a type hint (list[str], Sequence[str], etc)"
            )

    def get_input(self) -> I_co:
        return get_args(super().get_input())[0]

    @inject
    def collect_input(self, input: I, queue: IQueue) -> None:
        queue.queue_data(data=input)

    @inject
    def ready(self, queue: IQueue) -> bool:
        return (
            queue.queue_size >= self.batch_size
            or queue.time_since_first_item
            >= timedelta(seconds=self.max_batching_window)
        )

    @inject
    def fetch_batch(self, queue: IQueue) -> Sequence[I]:
        return queue.fetch(self.batch_size)

    def lambda_handler(self, event, context) -> Dict:
        bootstrap(CloudProvider.aws)
        print(event)
        print(context)
        input_type: I = self.get_input()
        return self(
            input=[
                input_type.parse_obj(json.loads(record.get("body")))
                for record in event["Records"]
            ],
        ).dict(by_alias=True, exclude_unset=True)


def step(*args, **kwargs) -> Callable[..., Step]:
    """
    Decorator to create a Step object from func.
    """

    def wrapper(func):
        return BasicStep(func=func, *args, **kwargs)

    return wrapper


def collector(*args, **kwargs) -> Callable[..., Step]:
    """
    Decorator to create a Step object from func.
    """

    def wrapper(func):
        return Collector(func=func, *args, **kwargs)

    return wrapper
