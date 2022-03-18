import os
from aws_cdk import (
    core,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_stepfunctions as sf,
)

from ingest.stack.constructs.triggers.trigger import TriggerConstruct


class S3TriggerConstruct(TriggerConstruct):
    from ingest.trigger import S3Trigger

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        pipeline_name: str,
        state_machine: sf.StateMachine,
        trigger: S3Trigger,
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
            f"s3_trigger_{pipeline_name}"[:79],
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "handler"),
            ),
            environment={"STATE_MACHINE_ARN": state_machine.state_machine_arn},
            timeout=core.Duration.seconds(30),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.handler",
        )
        state_machine.grant_start_execution(l)
        bucket = s3.Bucket.from_bucket_name(
            self, f"trigger_bucket_{pipeline_name}"[:79], trigger.bucket_name
        )
        bucket.grant_read(l)
        for event_type in trigger.events:
            print(f"Adding event notification for {event_type}")
            print(trigger.notification_key_filter_kwargs)
            print(bucket.bucket_name)
            bucket.add_event_notification(
                getattr(s3.EventType, event_type),
                s3n.LambdaDestination(l),
                s3.NotificationKeyFilter(**trigger.notification_key_filter_kwargs),
            )
