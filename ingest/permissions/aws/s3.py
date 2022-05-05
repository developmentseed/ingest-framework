from enum import Enum
from typing import Sequence
from ingest.permissions import Permission


class S3AccessActions(str, Enum):
    full = "full"
    delete = "delete"
    put = "put"
    read = "read"
    write = "read"


class S3Access(Permission):
    bucket_name: str
    actions: Sequence[S3AccessActions] = []

    def aws_grant(self, scope, func):
        from aws_cdk import aws_s3 as s3

        bucket = s3.Bucket.from_bucket_name(scope, "bucket", self.bucket_name)
        for action in self.actions:
            getattr(bucket, f"grant_{action}")(func)


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
