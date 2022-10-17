from datetime import timedelta
import json

from kink import inject

from ..services.queue import IQueue
from ..services.secrets import ISecrets

from typing import Sequence, TypeVar

T = TypeVar("T")


@inject(alias=IQueue)
class SqsQueue(IQueue[T]):
    def __init__(self, queue_name: str):
        import boto3

        sqs = boto3.resource("sqs")
        self.queue = sqs.Queue("TODO: Some URL")

    def fetch(self, num_items: int) -> Sequence[T]:
        # TODO: Parse response to ensure type T is returned
        return self.queue.receive_messages(MaxNumberOfMessages=num_items)

    def queue_data(self, data: T) -> None:
        # TODO: Make work...
        return self.queue.send_message(json.dumps(data))

    @property
    def queue_size(self) -> int:
        return self.queue.attributes["ApproximateNumberOfMessages"]

    @property
    def time_since_first_item(self) -> timedelta:
        # TODO: Make work...
        return timedelta(seconds=-1)


@inject(alias=ISecrets)
class SecretsManager(ISecrets):
    def __init__(self):
        import boto3

        self.secrets_client = boto3.client("secretsmanager")

    def get_secret_value(self, secret_id: str):
        return self.secrets_client.get_secret_value(SecretId=secret_id)["SecretString"]

    def get_secret(self, secret_name: str) -> str:
        return self.get_secret_value(secret_name)

    def get_secrets(self, secret_names: Sequence[str]) -> Sequence[str]:
        return [self.get_secret_value(secret_name) for secret_name in secret_names]
