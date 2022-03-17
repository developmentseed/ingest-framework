import logging
import os
from pathlib import Path
import subprocess
import sys
from typing import List, Optional, Sequence, Type
from aws_cdk import (
    core,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_stepfunctions as sf,
    aws_stepfunctions_tasks as tasks,
)
from ingest.permissions import S3Access

from ingest.step import Collector
from ingest.trigger import S3Trigger

logger = logging.getLogger(__name__)


class PipelineStack(core.Stack):
    from ingest.permissions import Permission
    from ingest.pipeline import Pipeline
    from ingest.step import Step, Collector
    from ingest.trigger import S3Trigger

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        code_dir: Path,
        requirements_path: Path,
        pipeline: Pipeline,
        steps: Sequence[Type[Step]],
        *args,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        layer = self.create_dependencies_layer()

        collectors = [
            (i, step) for i, step in enumerate(steps) if issubclass(step, Collector)
        ]

        starting_idx = 0
        trigger_queue = None
        for i, (idx, collector) in enumerate(
            collectors or [(len(steps), None)]  # type:ignore
        ):
            logger.debug(f"Creating workflow for steps {starting_idx} to {idx}")
            if collector:
                target_queue = sqs.Queue(
                    self,
                    self.collector_queue_name(collector),
                    queue_name=self.collector_queue_name(collector),
                    visibility_timeout=core.Duration.minutes(
                        11
                    ),  # TODO: make this configurable
                    receive_message_wait_time=core.Duration.seconds(
                        10
                    ),  # TODO: make this configurable
                )
            self.create_workflow(
                workflow_num=i,
                pipeline=pipeline,
                code_dir=code_dir,
                requirements_path=requirements_path,
                steps=steps[starting_idx:idx],
                layer=layer,
                collector=collector,  # type: ignore
                target_queue=target_queue,
                trigger_queue=trigger_queue,
            )
            starting_idx = idx
            trigger_queue = target_queue
        else:
            if starting_idx < len(
                steps
            ):  # There are remaining steps after the final collector
                logger.debug(f"Creating workflow for steps {starting_idx} to end")
                self.create_workflow(
                    workflow_num=i + 1,
                    pipeline=pipeline,
                    code_dir=code_dir,
                    requirements_path=requirements_path,
                    steps=steps[starting_idx:],
                    layer=layer,
                    trigger_queue=trigger_queue,
                )

    def create_workflow(
        self,
        workflow_num: int,
        pipeline: Pipeline,
        code_dir: Path,
        requirements_path: Path,
        steps: Sequence[Type[Step]],
        layer: lambda_.LayerVersion,
        collector: Optional[Type[Collector]] = None,
        target_queue: Optional[sqs.Queue] = None,
        trigger_queue: Optional[sqs.Queue] = None,
    ) -> sf.StateMachine:
        lambdas = self.create_lambda_tasks(
            steps=steps,
            code_dir=code_dir,
            requirements_path=requirements_path,
            layer=layer,
        )
        if collector and target_queue:
            # append lambda function to post input to SQS queue
            lambdas.append(
                self.create_collector_send_lambda(
                    queue_name=self.collector_queue_name(collector),
                    sqs_queue=target_queue,
                )
            )

        state_machine = self.create_state_machine(
            f"{pipeline.resource_name}{workflow_num}", lambdas
        )

        if workflow_num == 0:
            # set trigger to pipeline trigger
            if isinstance(pipeline.trigger, S3Trigger):
                self.create_s3_trigger_lambda(
                    pipeline_name=pipeline.name,
                    state_machine=state_machine,
                    trigger=pipeline.trigger,
                )
        elif (
            issubclass(steps[0], Collector) and trigger_queue
        ):  # this should always be true, if workflow_num > 0
            # set trigger to SQS with collector props
            self.create_sqs_trigger_lambda(
                state_machine,
                self.collector_queue_name(steps[0]),
                trigger_queue,
                steps[0],
            )

        return state_machine

    def create_s3_trigger_lambda(
        self, pipeline_name: str, state_machine: sf.StateMachine, trigger: S3Trigger
    ) -> lambda_.Function:
        l = lambda_.Function(
            self,
            f"s3_trigger_{pipeline_name}"[:79],
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "handlers", "s3_trigger"),
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
            bucket.add_event_notification(
                getattr(s3.EventType, event_type),
                s3n.LambdaDestination(l),
                s3.NotificationKeyFilter(**trigger.notification_key_filter_kwargs),
            )
        return l

    def create_sqs_trigger_lambda(
        self,
        state_machine: sf.StateMachine,
        queue_name: str,
        sqs_queue: sqs.Queue,
        collector: Type[Collector],
    ) -> lambda_.Function:
        l = lambda_.Function(
            self,
            f"consume_{queue_name}"[:79],
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "handlers", "sqs_trigger"),
            ),
            environment={
                "STATE_MACHINE_ARN": state_machine.state_machine_arn,
                "QUEUE_NAME": queue_name,
            },
            timeout=core.Duration.seconds(30),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.handler",
        )
        state_machine.grant_start_execution(l)
        sqs_queue.grant_consume_messages(l)
        l.add_event_source(
            event_sources.SqsEventSource(
                sqs_queue,
                batch_size=collector.batch_size,
                max_batching_window=core.Duration.seconds(
                    collector.max_batching_window
                ),
            )
        )
        return l

    def create_collector_send_lambda(
        self, queue_name: str, sqs_queue: sqs.Queue
    ) -> tasks.LambdaInvoke:
        l = lambda_.Function(
            self,
            f"send_to_{queue_name}"[:79],
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "handlers", "sqs_send"),
            ),
            environment={"QUEUE_URL": sqs_queue.queue_url},
            timeout=core.Duration.seconds(10),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.handler",
        )
        sqs_queue.grant_send_messages(l)
        return tasks.LambdaInvoke(
            self,
            f"task_send_to_{queue_name}"[:79],
            lambda_function=l,
            payload_response_only=True,
        )

    def create_lambda_tasks(
        self,
        steps: Sequence[Type[Step]],
        code_dir: Path,
        requirements_path: Path,
        layer: lambda_.LayerVersion,
    ) -> List[tasks.LambdaInvoke]:
        lambdas = []
        for step in steps:
            d = code_dir.relative_to(Path(os.path.curdir).resolve())

            if step.requirements_path:
                reqs = step.requirements_path.relative_to(code_dir)
            else:
                reqs = requirements_path.relative_to(code_dir)

            handler_file = self.get_handler_template_contents().format(
                handler_module=step.__module__, handler_class=step.__name__
            )
            lambda_name = step.__name__
            lambda_prefix = self.stack_name[: 79 - len(lambda_name)]

            handler_name = "handler"

            step_lambda = lambda_.Function(
                self,
                f"{lambda_prefix}-{lambda_name}",
                code=lambda_.Code.from_asset(
                    str(d.absolute()),
                    exclude=["__pycache__"],
                    bundling=core.BundlingOptions(
                        image=lambda_.Runtime.PYTHON_3_9.bundling_image,
                        command=[
                            "bash",
                            "-c",
                            f'echo "{handler_file}" > /asset-output/{handler_name}.py && pip install -r {reqs} -t /asset-output && cp -au . /asset-output/{d}',
                        ],
                    ),
                ),
                handler=f"{handler_name}.handler",
                timeout=core.Duration.minutes(1),
                runtime=lambda_.Runtime.PYTHON_3_9,
                layers=[layer],
            )

            for permission in step.permissions:
                self.grant_permission(permission, step_lambda)

            # TODO: Add error handling and retry config

            lambda_task = tasks.LambdaInvoke(
                self,
                lambda_name[:79],
                lambda_function=step_lambda,
                payload_response_only=True,
            )

            lambdas.append(lambda_task)
        return lambdas

    def grant_permission(self, permission: Permission, func: lambda_.Function):
        if isinstance(permission, S3Access):
            bucket = s3.Bucket.from_bucket_name(func, "bucket", permission.bucket_name)
            for action in permission.actions:
                getattr(bucket, f"grant_{action}")(func)

    def create_state_machine(
        self, state_machine_name: str, lambdas: Sequence[tasks.LambdaInvoke]
    ) -> sf.StateMachine:
        state_machine_prefix = self.stack_name[: 79 - len(state_machine_name)]
        definition = sf.Chain.start(lambdas[0])
        for l in lambdas[1:]:
            definition = definition.next(l)
        return sf.StateMachine(
            self,
            state_machine_name,
            state_machine_name=f"{state_machine_prefix}_{state_machine_name}".lower(),
            definition=definition.next(
                sf.Succeed(
                    self, f"Complete-{state_machine_name}"[:79], comment="Complete"
                )
            ),
        )

    def get_handler_template_contents(self):
        template_file_path = os.path.join(
            os.path.dirname(__file__), "templates/handler.py.template"
        )
        with open(template_file_path, "r") as f:
            return f.read()

    def create_dependencies_layer(
        self,
    ) -> lambda_.LayerVersion:
        dir_name = os.path.join(".ingest-build", ".dependency_layer")
        try:
            os.makedirs(dir_name)
        except FileExistsError:
            pass
        # with TemporaryDirectory() as tmp_dir:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--upgrade",
                os.path.join(os.path.dirname(__file__), ".."),
                "-t",
                f"{dir_name}/python/lib/python3.9/site-packages/",
            ]
        )
        layer = lambda_.LayerVersion(
            self,
            "IngestDependencies",
            code=lambda_.Code.from_asset(dir_name),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
            description="The ingest framework",
        )
        return layer

    @staticmethod
    def collector_queue_name(collector: Type[Collector]):
        # this needs some protection against reusing
        # the same collector class within one pipeline
        return f"{collector.__name__}_queue"
