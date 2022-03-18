from typing import List, Optional, Type
from pydantic import BaseModel

from ingest.data_types import S3Object
from ingest.provider import CloudProvider


class Trigger(BaseModel):
    output_type: Type

    def get_construct(self, provider: CloudProvider):
        raise NotImplementedError()


class S3Filter(BaseModel):
    prefix: Optional[str]
    suffix: Optional[str]


class S3Trigger(Trigger):
    bucket_name: str
    events: List[str]
    object_filter: S3Filter

    def get_construct(self, provider: CloudProvider):
        if provider == CloudProvider.aws:
            from ingest.stack.constructs.triggers.s3_trigger import S3TriggerConstruct

            return S3TriggerConstruct
        else:
            return super().get_construct(provider)

    @property
    def notification_key_filter_kwargs(self):
        return self.object_filter.dict(exclude_unset=True)


class S3ObjectCreated(S3Trigger):
    bucket_name: str
    events: List[str] = ["OBJECT_CREATED"]
    output_type: Type = S3Object


class SQSTrigger(Trigger):
    queue_name: str
    batch_size: int
    max_batching_window: int

    def get_construct(self, provider: CloudProvider):
        if provider == CloudProvider.aws:
            from ingest.stack.constructs.triggers.sqs_trigger import SQSTriggerConstruct

            return SQSTriggerConstruct
        else:
            return super().get_construct(provider)
