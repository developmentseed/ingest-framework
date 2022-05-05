from collections import deque
from datetime import datetime, timedelta
import os
from typing import Deque, Sequence, TypeVar

from kink import inject

from ingest.services.secrets import ISecrets
from ingest.services.queue import IQueue


T = TypeVar("T")

local_queue: Deque = deque()


@inject(alias=IQueue)
class LocalQueue(IQueue[T]):
    def __init__(self):
        self.queue = local_queue

    def fetch(self, num_items: int) -> Sequence[T]:
        return [self.queue.popleft()[1] for _ in range(min(num_items, self.queue_size))]

    def queue_data(self, data: T) -> None:
        return self.queue.append((datetime.now(), data))

    @property
    def time_since_first_item(self) -> timedelta:
        return datetime.now() - self.queue[0][0]

    @property
    def queue_size(self) -> int:
        return len(self.queue)


@inject(alias=ISecrets)
class SecretsManager(ISecrets):
    def get_secret_value(self, secret_id: str):
        return os.getenv(secret_id)

    def get_secret(self, secret_name: str) -> str:
        return self.get_secret_value(secret_name)

    def get_secrets(self, secret_names: Sequence[str]) -> Sequence[str]:
        return [self.get_secret_value(secret_name) for secret_name in secret_names]
