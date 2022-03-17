import boto3
import json
import logging
import os


class FailedToWriteToSQS(Exception):
    pass


logger = logging.getLogger(__name__)


def handler(event, context) -> None:
    sqs = boto3.client("sqs")
    response = sqs.send_message(
        QueueUrl=os.environ["QUEUE_URL"], MessageBody=json.dumps(event)
    )
    if response.get("Error"):
        logger.error(response.get("Error"))
        raise FailedToWriteToSQS(response.get("Error"))
    else:
        logger.info(f"Queued Item")
        return response.get("MessageId")
