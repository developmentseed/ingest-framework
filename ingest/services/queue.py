from abc import abstractmethod, abstractproperty
from datetime import timedelta
from typing import Protocol, Sequence, TypeVar

T = TypeVar("T")


class IQueue(Protocol[T]):
    @abstractmethod
    def fetch(self, num_items: int) -> Sequence[T]:
        ...

    @abstractmethod
    def queue_data(self, data: T) -> None:
        ...

    @abstractproperty
    def queue_size(self) -> int:
        ...

    @abstractproperty
    def time_since_first_item(self) -> timedelta:
        ...
