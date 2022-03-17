from pydantic import BaseModel


class S3Object(BaseModel):
    bucket: str
    key: str
