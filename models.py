from collections import deque
from datetime import datetime, timedelta

from inspect import signature
from typing import (
    Deque,
    Sequence,
    Optional,
    get_args,
    Any,
    Dict,
    Protocol,
    Generic,
    List,
    Type,
    TypeVar,
)
from uuid import uuid4
from pydantic import UUID4, BaseModel

I = TypeVar("I")
O = TypeVar("O", covariant=True)
I_contra = TypeVar("I_contra", contravariant=True)
I_co = TypeVar("I_co", covariant=True)
EI = TypeVar("EI")
EO = TypeVar("EO")
TO = TypeVar("TO")
LO = TypeVar("LO")


def get_base(cls):
    """Utility for retrieving the base class"""
    if hasattr(cls, "__orig_bases__"):
        return get_base(cls.__orig_bases__[0])
    else:
        return cls


class BatchCache:
    cache: Deque

    def __init__(self):
        self.cache = deque()

    def fetch(self, num_items: int):
        return [self.cache.popleft()[1] for _ in range(min(num_items, self.queue_size))]

    def queue_data(self, data: Any):
        self.cache.append((datetime.now(), data))

    @property
    def queue_size(self):
        return len(self.cache)

    def time_since_first_item(self):
        return datetime.now() - self.cache[0][0]


# Data classes
class S3Item(BaseModel):
    bucket: str
    key: str


class SpireItem(BaseModel):
    id: int
    name: str
    location: str


class PlanetItem(BaseModel):
    id: int
    name: str
    something: str


class StacItem(BaseModel):
    id: int
    properties: Dict


class Nothing(BaseModel):
    pass


# Pipeline Steps
class PipelineStep(Protocol[I_co, O]):
    uuid: UUID4
    input_type: Type[I_co]
    output_type: Type[O]

    def __init__(self):
        self.uuid = uuid4()
        self.input_type = I_co
        self.output_type = O

    def get_output(self) -> O:
        return get_args(get_base(self))[1]

    def get_input(self) -> I_co:
        return get_args(get_base(self))[0]


class Transform(PipelineStep[I, O]):
    def execute(self, input: I, *args, **kwargs) -> O:
        raise NotImplementedError()


class Collector(PipelineStep[I, O]):
    batch_size: int
    max_batching_window: int
    cache: BatchCache

    def __init__(self, batch_size: int = 100, max_batching_window: int = 60):
        super().__init__()
        self.batch_size = batch_size
        self.max_batching_window = max_batching_window
        self.cache = BatchCache()

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

    def execute(self, input: Sequence[I], *args, **kwargs) -> O:
        raise NotImplementedError()


class PipelineFunc(Protocol[I_contra, O]):
    def __call__(self, input: I_contra, *args, **kwargs) -> O:
        raise NotImplementedError()


class PS(Generic[I, O]):
    _prev: Optional["PS[Any,I]"]
    _next: Optional["PS[O, Any]"]
    handler: PipelineFunc

    def __init__(
        self,
        handler: PipelineFunc,
        _prev: Optional["PS[Any,I]"] = None,
        _next: Optional["PS[O, Any]"] = None,
    ):
        self.handler = handler
        self._prev = _prev
        self._next = _next


class FakeExtractSpire(PipelineStep[S3Item, SpireItem]):
    def execute(self, input: S3Item, *args, **kwargs) -> SpireItem:
        return SpireItem(id=1, name="test", location=input.key)


class FakeExtractPlanet(PipelineStep[S3Item, PlanetItem]):
    def execute(self, input: S3Item, *args, **kwargs) -> PlanetItem:
        return PlanetItem(id=1, name="test", something=input.bucket)


class DoNothing(PipelineStep[StacItem, Nothing]):
    def execute(self, input: StacItem, *args, **kwargs) -> Nothing:
        return Nothing()


class SpireToStacTransform(PipelineStep[SpireItem, StacItem]):
    def execute(self, input: SpireItem, *args, **kwargs) -> StacItem:
        return StacItem(
            id=input.id,
            properties={
                "name": input.name,
                "location": input.location,
            },
        )


class LoadToPgstac(Collector[StacItem, StacItem]):
    def execute(self, input: Sequence[StacItem], *args, **kwargs) -> StacItem:
        for item in input:
            print(f"Loading {item}")
        return input[0]


class StaticPipeline(Generic[EI, EO, TO, LO]):
    """
    This implementation of Pipeline *can* statically verify that
    the output of the Extract step matches the input of the
    Transform step and so on.

    BUT, it has a number of significant drawbacks:
    1. The number of steps in the pipeline is limited (granted,
    you can just cram a bunch of function calls into each step,
    but it feels like this shouldn't be a limitation).
    2. It violates DRY, since we have to declare the input
    and output types on both the StaticPipeline class and
    each PipelineStep class.
    """

    extract: Type[PipelineStep[EI, EO]]
    transform: Type[PipelineStep[EO, TO]]
    load: Type[PipelineStep[TO, LO]]

    def __init__(
        self,
        extract: Type[PipelineStep[EI, EO]],
        transform: Type[PipelineStep[EO, TO]],
        load: Type[PipelineStep[TO, LO]],
    ):
        self.extract = extract
        self.transform = transform
        self.load = load


valid_pipeline = StaticPipeline[S3Item, SpireItem, StacItem, Nothing](
    FakeExtractSpire, SpireToStacTransform, DoNothing
)

# invalid_pipeline = StaticPipeline[S3Item, SpireItem, StacItem, Nothing](
#     FakeExtractPlanet,
#     SpireToStacTransform,
#     DoNothing,  # This correctly displays an error during static type checking
# )


class DynamicPipeline:
    """
    This implementation does not support statically checking the input/output
    types of each step. That validation can only happen at runtime.

    But it has some signficant benefits:
    1. Supports an arbitrary number of PipelineSteps.
    2. Does not require us to define the input/output types of each step twice.
    """

    uuid: UUID4

    def __init__(self, steps: List[PipelineStep]):
        self.uuid = uuid4()
        self.steps = steps
        self.validate()

    def run(self, input):
        for step in self.steps:
            if isinstance(step, Collector):
                step.collect_input(input)
                if step.ready():
                    step.execute(input=step.fetch_batch())
                else:
                    return input
            else:
                input = step.execute(input)
        return input

    def validate(self):
        i = 0
        while i < len(self.steps) - 1:
            output_type = self.steps[i].get_output()
            input_type = self.steps[i + 1].get_input()
            assert (
                output_type == input_type
            ), f"Output of step {i} ({output_type} is not equal to input of step {i+1} ({input_type})"
            i += 1


valid_pipe = DynamicPipeline(
    steps=[FakeExtractSpire(), SpireToStacTransform(), LoadToPgstac(batch_size=2)]
)

# invalid_pipe = DynamicPipeline(  # this will correctly fail at runtime, but does not display an error in static type checking
#     steps=[
#         FakeExtractPlanet,
#         SpireToStacTransform,
#         DoNothing,
#     ]
# )


class FuncPipeline:
    """
    Another pipeline where input/output types are validated at runtime, but in this
    case the steps are simple functions that comply with the PipelineFunc protocol.

    Same disadvantage as the DynamicPipeline above, but defining each step as a
    function saves some boilerplate.
    """

    steps: List[PipelineFunc]

    def __init__(self, steps: List[PipelineFunc]):
        self.steps = steps
        self.validate()

    def run(self, input):
        for step in self.steps:
            input = step(input)
        return input

    def validate(self):
        i = 0
        while i < len(self.steps) - 1:
            output_type = signature(self.steps[i]).return_annotation
            input_type = signature(self.steps[i + 1]).parameters["input"].annotation
            assert (
                output_type == input_type
            ), f"Output of step {i} ({output_type} is not equal to input of step {i+1} ({input_type})"
            i += 1


# Pipeline functions
def s3_to_spire(input: S3Item, *args, **kwargs) -> SpireItem:
    return SpireItem(id=1, name="test", location=input.key)


def s3_to_planet(input: S3Item, *args, **kwargs) -> PlanetItem:
    return PlanetItem(id=1, name="test", something=input.bucket)


def spire_to_stac(input: SpireItem, *args, **kwargs) -> StacItem:
    return StacItem(
        id=input.id,
        properties={
            "name": input.name,
            "location": input.location,
        },
    )


def do_nothing(input: StacItem, *args, **kwargs) -> Nothing:
    return Nothing()


p = FuncPipeline(steps=[s3_to_spire, spire_to_stac])


# class StaticFuncPipeline:
#     """
#     Another pipeline where input/output types are validated at runtime, but in this
#     case the steps are simple functions that comply with the PipelineFunc protocol.

#     Same disadvantage as the DynamicPipeline above, but defining each step as a
#     function saves some boilerplate.
#     """

#     steps: List[PS] = []

#     def __init__(self, steps: List[PipelineFunc]):
#         ps = []
#         for i, step in enumerate(steps):
#             input_type = Type[signature(step).parameters["input"].annotation]
#             output_type = Type[signature(step).return_annotation]
#             ps.append(PS[input_type, output_type](handler=step))

#         for i, pstep in enumerate(ps):
#             # p = ps[i - 1] if i > 0 else None
#             n = ps[i + 1] if i < len(ps) - 1 else None
#             pstep._next = n
#             if n:
#                 n._prev = pstep
#             self.steps.append(p)

#         self.validate()

#     def run(self, input):
#         for step in self.steps:
#             input = step(input)
#         return input

#     def validate(self):
#         i = 0
#         while i < len(self.steps) - 1:
#             output_type = signature(self.steps[i]).return_annotation
#             input_type = signature(self.steps[i + 1]).parameters["input"].annotation
#             assert (
#                 output_type == input_type
#             ), f"Output of step {i} ({output_type} is not equal to input of step {i+1} ({input_type})"
#             i += 1
