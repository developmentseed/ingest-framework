import os
import subprocess
import sys
from tempfile import TemporaryDirectory
from typing import Sequence, Type
from aws_cdk import (
    core,
    aws_lambda as lambda_,
    aws_stepfunctions as sf,
    aws_stepfunctions_tasks as tasks,
)


class PipelineStack(core.Stack):
    from ingest.pipeline import Pipeline
    from ingest.step import Step

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        code_dir: str,
        requirements_path: str,
        pipeline: Pipeline,
        steps: Sequence[Type[Step]],
        *args,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        lambdas = self.create_lambda_tasks(
            steps=steps, code_dir=code_dir, requirements_path=requirements_path
        )

        state_machine = self.create_state_machine(pipeline, lambdas)

    def create_lambda_tasks(
        self, steps: Sequence[Type[Step]], code_dir: str, requirements_path: str
    ) -> Sequence[tasks.LambdaInvoke]:
        layer = self.create_dependencies_layer()
        lambdas = []
        for step in steps:
            d = os.path.relpath(code_dir)
            reqs = os.path.join(d, requirements_path)

            handler_file = self.get_handler_template_contents().format(
                handler_module=step.__module__, handler_class=step.__name__
            )
            lambda_name = step.__name__
            lambda_prefix = self.stack_name[: 79 - len(lambda_name)]

            step_lambda = lambda_.Function(
                self,
                f"{lambda_prefix}-{lambda_name}",
                code=lambda_.Code.from_asset(
                    ".",
                    bundling=core.BundlingOptions(
                        image=lambda_.Runtime.PYTHON_3_9.bundling_image,
                        command=[
                            "bash",
                            "-c",
                            f'echo "{handler_file}" > /asset-output/lambda_handler.py && pip install -r {reqs} -t /asset-output && cp -au {d} /asset-output',
                        ],
                    ),
                ),
                handler="lambda_handler.handler",
                timeout=core.Duration.minutes(1),
                runtime=lambda_.Runtime.PYTHON_3_9,
                layers=[layer],
            )

            # TODO: Add error handling and retry config

            lambda_task = tasks.LambdaInvoke(
                self,
                lambda_name[:79],
                lambda_function=step_lambda,
                payload_response_only=True,
            )

            lambdas.append(lambda_task)
        return lambdas

    def create_state_machine(
        self, pipeline: Pipeline, lambdas: Sequence[tasks.LambdaInvoke]
    ) -> sf.StateMachine:
        state_machine_name = pipeline.resource_name
        state_machine_prefix = self.stack_name[: 79 - len(state_machine_name)]
        definition = sf.Chain.start(lambdas[0])
        for l in lambdas[1:]:
            definition = definition.next(l)
        return sf.StateMachine(
            self,
            state_machine_name,
            state_machine_name=f"{state_machine_prefix}_{state_machine_name}".lower(),
            definition=definition.next(sf.Succeed(self, "Complete")),
        )

    def get_handler_template_contents(self):
        template_file_path = os.path.join(
            os.path.dirname(__file__), "templates/handler.py.template"
        )
        with open(template_file_path, "r") as f:
            return f.read()

    @staticmethod
    def get_path(step):
        return f"{step.__module__}.{step.__name__}"

    def create_dependencies_layer(
        self,
    ) -> lambda_.LayerVersion:
        with TemporaryDirectory() as tmp_dir:
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    os.path.join(os.path.dirname(__file__), ".."),
                    "-t",
                    f"{tmp_dir}/python/lib/python3.9/site-packages/",
                ]
            )
            layer = lambda_.LayerVersion(
                self,
                "IngestDependencies",
                code=lambda_.Code.from_asset(tmp_dir),
                compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
                description="The ingest framework",
            )
        return layer
