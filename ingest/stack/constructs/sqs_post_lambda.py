import logging
import os

from aws_cdk import (
    Duration,
    aws_lambda as lambda_,
    aws_sqs as sqs,
)
from constructs import Construct

logger = logging.getLogger(__name__)


class SQSQueuePostLambda(lambda_.Function):
    def __init__(self, scope: Construct, queue_name: str, sqs_queue: sqs.Queue):
        super().__init__(
            scope,
            f"send_to_{queue_name}"[:79],
            code=lambda_.Code.from_asset(
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "handlers", "sqs_send"
                ),
            ),
            environment={"QUEUE_URL": sqs_queue.queue_url},
            timeout=Duration.seconds(10),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.handler",
        )
        sqs_queue.grant_send_messages(self)
