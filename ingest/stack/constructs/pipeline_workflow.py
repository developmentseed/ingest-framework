import logging

from pathlib import Path
from typing import List, Optional, Sequence
from aws_cdk import (
    aws_lambda as lambda_,
    aws_sqs as sqs,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct
from ingest.providers import CloudProvider
from ingest.stack.constructs.step_lambda import StepLambda
from ingest.stack.constructs.pipeline_state_machine import PipelineStateMachine
from ingest.stack.constructs.sqs_post_lambda import SQSQueuePostLambda
from ingest.stack.naming import collector_queue_name

from ingest.step import Collector
from ingest.trigger import SQSTrigger

logger = logging.getLogger(__name__)


class PipelineWorkflow(Construct):
    from ingest.pipeline import Pipeline
    from ingest.step import Step, Collector

    def __init__(
        self,
        scope: Construct,
        id: str,
        workflow_num: int,
        pipeline: Pipeline,
        code_dir: Path,
        requirements_path: Path,
        steps: Sequence[Step],
        layer: lambda_.LayerVersion,
        collector: Optional[Collector] = None,
        target_queue: Optional[sqs.Queue] = None,
        trigger_queue: Optional[sqs.Queue] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        lambdas = self.create_lambda_tasks(
            steps=steps,
            code_dir=code_dir,
            requirements_path=requirements_path,
            layer=layer,
        )
        if collector and target_queue:
            queue_name = collector_queue_name(pipeline, collector)
            # append lambda function to post input to SQS queue
            collector_send_lambda = SQSQueuePostLambda(
                self, queue_name=queue_name, sqs_queue=target_queue
            )

            lambdas.append(
                tasks.LambdaInvoke(
                    self,
                    f"task_send_to_{queue_name}"[:79],
                    lambda_function=collector_send_lambda,
                    payload_response_only=True,
                )
            )

        self.state_machine = PipelineStateMachine(
            self,
            f"StateMachine{workflow_num}",
            f"{pipeline.resource_name}{workflow_num}",
            lambdas,
        )

        if workflow_num == 0:
            # set trigger to pipeline trigger
            print("Creating pipeline trigger")
            pipeline.trigger.get_construct(provider=CloudProvider.aws)(
                self,
                "PipelineTrigger",
                pipeline_name=pipeline.name,
                state_machine=self.state_machine,
                trigger=pipeline.trigger,
            )
        elif (
            isinstance(steps[0], Collector) and trigger_queue
        ):  # this should always be true, if workflow_num > 0
            # set trigger to SQS with collector props
            step = steps[0]
            trigger = SQSTrigger(
                output_type=step.get_output(),
                queue_name=collector_queue_name(pipeline, step),
                batch_size=step.batch_size,
                max_batching_window=step.max_batching_window,
            )
            trigger.get_construct(provider=CloudProvider.aws)(
                self,
                f"SQSTrigger{workflow_num}",
                pipeline_name=pipeline.name,
                state_machine=self.state_machine,
                trigger=trigger,
                sqs_queue=trigger_queue,
            )

    def create_lambda_tasks(
        self,
        steps: Sequence[Step],
        code_dir: Path,
        requirements_path: Path,
        layer: lambda_.LayerVersion,
    ) -> List[tasks.LambdaInvoke]:
        lambdas = []
        for i, step in enumerate(steps):
            step_lambda = StepLambda(
                self,
                f"Step{i}",
                step=step,
                code_dir=code_dir,
                default_requirements_path=requirements_path,
                base_layer=layer,
            )

            # TODO: Add error handling and retry config

            lambda_task = tasks.LambdaInvoke(
                self,
                step_lambda.lambda_name[:79],
                lambda_function=step_lambda,
                payload_response_only=True,
            )

            lambdas.append(lambda_task)
        return lambdas
