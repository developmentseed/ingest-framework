from typing import Dict, Generic, Optional, Type, TypeVar


T = TypeVar("T")


class Secret(Generic[T]):
    secret_id: str
    secret_value: Optional[T] = None

    def __init__(self, secret_id: str, secret_value: Optional[T] = None):
        self.secret_id = secret_id
        self.secret_value = secret_value


SecretsMap = Dict[str, Secret]
