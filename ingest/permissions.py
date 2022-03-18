from enum import Enum
from typing import Sequence
from pydantic import BaseModel


class S3AccessActions(str, Enum):
    full = "full"
    delete = "delete"
    put = "put"
    read = "read"
    write = "write"


class Permission(BaseModel):
    pass


class S3Access(Permission):
    bucket_name: str
    actions: Sequence[S3AccessActions] = []


class S3ReadAccess(S3Access):
    bucket_name: str
    actions: Sequence[S3AccessActions] = [S3AccessActions.read]


class S3WriteAccess(S3Access):
    bucket_name: str
    actions: Sequence[S3AccessActions] = [S3AccessActions.write]


class S3PutAccess(S3Access):
    bucket_name: str
    actions: Sequence[S3AccessActions] = [S3AccessActions.put]


class S3DeleteAccess(S3Access):
    bucket_name: str
    actions: Sequence[S3AccessActions] = [S3AccessActions.delete]


class S3FullAccess(S3Access):
    bucket_name: str
    actions: Sequence[S3AccessActions] = [S3AccessActions.full]
