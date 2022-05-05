from typing import Dict
from pydantic import BaseModel
from ingest.step import step
from ingest.data_types import S3Object


class StacItem(BaseModel):
    id: str
    properties: Dict


@step()
def s3_to_stac(input: S3Object) -> StacItem:
    return StacItem(
        id=f"{input.bucket}-{input.key}",
        properties={"bucket": input.bucket, "key": input.key},
    )


@step()
def stac_to_s3(input: StacItem) -> S3Object:
    return S3Object(
        bucket=input.properties.get("bucket"), key=input.properties.get("key")
    )
