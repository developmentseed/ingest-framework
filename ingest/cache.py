from collections import deque
from datetime import datetime
from typing import Any, Deque


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
