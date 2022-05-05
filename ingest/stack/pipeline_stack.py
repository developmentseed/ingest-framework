import logging
import os
from pathlib import Path
import subprocess
import sys
from typing import Sequence, Type
from aws_cdk import (
    core,
    aws_lambda as lambda_,
    aws_sqs as sqs,
)

from ingest.stack.constructs.pipeline_workflow import PipelineWorkflow
from ingest.stack.naming import collector_queue_name

logger = logging.getLogger(__name__)


class PipelineStack(core.Stack):
    from ingest.pipeline import Pipeline
    from ingest.step import Step

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        code_dir: Path,
        requirements_path: Path,
        pipeline: Pipeline,
        steps: Sequence[Step],
        *args,
        **kwargs,
    ):
        from ingest.step import Collector

        super().__init__(scope, id, **kwargs)

        layer = self.create_dependencies_layer()

        collectors = [
            (i, step) for i, step in enumerate(steps) if isinstance(step, Collector)
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
                    collector_queue_name(pipeline, collector),
                    queue_name=collector_queue_name(pipeline, collector),
                    visibility_timeout=core.Duration.minutes(
                        11
                    ),  # TODO: make this configurable
                    receive_message_wait_time=core.Duration.seconds(
                        10
                    ),  # TODO: make this configurable
                )
            PipelineWorkflow(
                self,
                f"Workflow{i}",
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
                PipelineWorkflow(
                    self,
                    f"Workflow{i+1}",
                    workflow_num=i + 1,
                    pipeline=pipeline,
                    code_dir=code_dir,
                    requirements_path=requirements_path,
                    steps=steps[starting_idx:],
                    layer=layer,
                    trigger_queue=trigger_queue,
                )

    def create_dependencies_layer(
        self,
    ) -> lambda_.LayerVersion:
        dir_name = os.path.join(".ingest-build", ".dependency_layer")
        try:
            os.makedirs(dir_name)
        except FileExistsError:
            pass

        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--upgrade",
                os.path.join(os.path.dirname(__file__), "..", ".."),
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
