import datetime
import json
import logging
import os
from uuid import uuid4
import boto3
from botocore.config import Config


def prepare_execution_name(name: str) -> str:
    """
    Given the name of an execution, return a sanitized name that has been
    appended with a suffix unique to the date of execution and has been trimmed
    to remain under 81 characters.
    """
    # TODO: Make this more meaningful but still unique
    return uuid4().hex
    # suffix = datetime.datetime.now().strftime("%y%m%d")
    # cleaned_name = name.replace("/", "-")
    # return f"{cleaned_name}{suffix}"[-80:]


class StepFunctionThrottled(Exception):
    pass


class StepFunctionValidationException(Exception):
    pass


logger = logging.getLogger(__name__)


def handler(event, context) -> None:
    client = boto3.client(
        "stepfunctions", config=Config(retries={"max_attempts": 10, "mode": "standard"})
    )
    try:
        response = client.start_execution(
            stateMachineArn=os.environ["STATE_MACHINE_ARN"],
            name=prepare_execution_name(os.environ["QUEUE_NAME"]),
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
