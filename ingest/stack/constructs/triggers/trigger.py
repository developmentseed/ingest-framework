from aws_cdk import aws_stepfunctions as sf
from constructs import Construct


class TriggerConstruct(Construct):
    from ingest.trigger import Trigger

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        pipeline_name: str,
        state_machine: sf.StateMachine,
        trigger: Trigger,
        **kwargs,
    ):
        super().__init__(scope, id)
