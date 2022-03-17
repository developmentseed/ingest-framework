from typing import Dict
from pydantic import BaseModel
from ingest.step import Transformer
from ingest.data_types import S3Object


class StacItem(BaseModel):
    id: str
    properties: Dict


class S3ToStac(Transformer[S3Object, StacItem]):
    @classmethod
    def execute(cls, input: S3Object) -> StacItem:
        return StacItem(
            id=f"{input.bucket}-{input.key}",
            properties={"bucket": input.bucket, "key": input.key},
        )


class StacToS3(Transformer[StacItem, S3Object]):
    @classmethod
    def execute(cls, input: StacItem) -> S3Object:
        return S3Object(
            bucket=input.properties.get("bucket"), key=input.properties.get("key")
        )
