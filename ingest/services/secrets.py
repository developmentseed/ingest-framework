from abc import abstractmethod, abstractproperty
from datetime import timedelta
from typing import Sequence, TypeVar

T = TypeVar("T")


class ISecrets:
    @abstractmethod
    def get_secret(self, secret_name: str) -> str:
        ...

    @abstractmethod
    def get_secrets(self, secret_names: Sequence[str]) -> Sequence[str]:
        ...
