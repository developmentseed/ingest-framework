from typing import Sequence

from aws_cdk import (
    aws_stepfunctions as sf,
    aws_stepfunctions_tasks as tasks,
)

from constructs import Construct

class PipelineStateMachine(sf.StateMachine):
    def __init__(
        self,
        scope: Construct,
        id: str,
        state_machine_name: str,
        lambdas: Sequence[tasks.LambdaInvoke],
    ):
        state_machine_prefix = id[: 79 - len(state_machine_name)]
        definition = sf.Chain.start(lambdas[0])
        for l in lambdas[1:]:
            definition = definition.next(l)
        super().__init__(
            scope,
            state_machine_name,
            state_machine_name=f"{state_machine_prefix}_{state_machine_name}".lower(),
            definition=definition.next(
                sf.Succeed(scope, f"Complete-{state_machine_name}", comment="Complete")
            ),
        )
