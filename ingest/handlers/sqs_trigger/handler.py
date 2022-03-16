import json
import logging
import os
from uuid import uuid4
import boto3
from botocore.config import Config


class StepFunctionThrottled(Exception):
    pass


class StepFunctionValidationException(Exception):
    pass


logger = logging.getLogger(__name__)


def handler(event, context):
    client = boto3.client(
        "stepfunctions", config=Config(retries={"max_attempts": 10, "mode": "standard"})
    )
    try:
        response = client.start_execution(
            stateMachineArn=os.environ["STATE_MACHINE_ARN"],
            name=uuid4().hex,  # TODO: replace this with something more meaningful. Maybe ULID or some combination of datetime and UUID.
            input=json.dumps(event),
        )
        logger.debug(response)
    except client.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ThrottlingException":
            raise StepFunctionThrottled(str(e)) from e
        elif code == "ValidationException":
            raise StepFunctionValidationException(str(e)) from e
        else:
            raise e
