from aws_cdk import core, aws_stepfunctions as sf


class TriggerConstruct(core.Construct):
    from ingest.trigger import Trigger

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        *,
        pipeline_name: str,
        state_machine: sf.StateMachine,
        trigger: Trigger,
        **kwargs,
    ):
        super().__init__(scope, id)
