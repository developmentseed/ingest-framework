import os
from aws_cdk import (
    core,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_stepfunctions as sf,
)

from ingest.stack.constructs.triggers.trigger import TriggerConstruct


class SQSTriggerConstruct(TriggerConstruct):
    from ingest.trigger import SQSTrigger

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        pipeline_name: str,
        state_machine: sf.StateMachine,
        trigger: SQSTrigger,
        sqs_queue: sqs.Queue,
        **kwargs,
    ):
        super().__init__(
            scope,
            id,
            pipeline_name=pipeline_name,
            state_machine=state_machine,
            trigger=trigger,
            **kwargs,
        )
        l = lambda_.Function(
            self,
            f"consume_{trigger.queue_name}"[:79],
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "handler"),
            ),
            environment={
                "STATE_MACHINE_ARN": state_machine.state_machine_arn,
                "QUEUE_NAME": trigger.queue_name,
            },
            timeout=core.Duration.seconds(30),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.handler",
        )
        state_machine.grant_start_execution(l)
        # sqs_queue = sqs.Queue.from_queue_attributes(
        #     self, "sqs_queue", queue_name=trigger.queue_name
        # )
        sqs_queue.grant_consume_messages(l)
        l.add_event_source(
            event_sources.SqsEventSource(
                sqs_queue,
                batch_size=trigger.batch_size,
                max_batching_window=core.Duration.seconds(trigger.max_batching_window),
            )
        )
