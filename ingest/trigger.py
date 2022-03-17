from typing import List, Optional, Type
from pydantic import BaseModel

from ingest.data_types import S3Object


class Trigger(BaseModel):
    output_type: Type
    pass


class S3Filter(BaseModel):
    prefix: Optional[str]
    suffix: Optional[str]


class S3Trigger(Trigger):
    bucket_name: str
    events: List[str]
    object_filter: S3Filter

    @property
    def notification_key_filter_kwargs(self):
        return self.object_filter.dict(exclude_unset=True)


class S3ObjectCreated(S3Trigger):
    bucket_name: str
    events: List[str] = ["OBJECT_CREATED"]
    output_type: Type = S3Object
