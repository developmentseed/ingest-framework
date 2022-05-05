import os

from inspect import signature
from pathlib import Path
from typing import Any, Dict, Mapping, get_args
from aws_cdk import core, aws_lambda as lambda_, aws_s3 as s3

from ingest.providers import CloudProvider


def format_lambda_properties(props: Mapping[str, Any]) -> Dict[str, Any]:
    ignored_lambda_properties = ("handler", "runtime", "layers", "code")
    formatted_props = {
        k: v for k, v in props.items() if k not in ignored_lambda_properties
    }
    func_sig = signature(lambda_.Function.__init__)
    for k, v in formatted_props.items():
        if k in func_sig.parameters:
            param_type = func_sig.parameters[k].annotation
            if core.Duration is param_type or core.Duration in get_args(param_type):
                formatted_props[k] = core.Duration.seconds(v)
        else:
            raise ValueError(f"{k} is an invalid property to pass to AWS Lambda")
    return formatted_props


class StepLambda(lambda_.Function):
    from ingest.step import Step

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        step: Step,
        code_dir: Path,
        default_requirements_path: Path,
        base_layer: lambda_.ILayerVersion,
        **kwargs,
    ):
        d = code_dir.relative_to(Path(os.path.curdir).resolve())

        step_reqs = " ".join(step.requirements)
        requirements_path = default_requirements_path.relative_to(code_dir)

        handler_file = self.get_handler_template_contents().format(
            handler_module=step.func.__module__, handler_class=step.name
        )

        self.lambda_name = step.name
        lambda_prefix = id[: 79 - len(self.lambda_name)]

        handler_name = "handler"

        step_reqs_cmd = (
            f"&& pip install {step_reqs} -t /asset-output" if step_reqs else ""
        )

        lambda_properties: Dict[str, Any] = {
            "timeout": core.Duration.minutes(1),
            "environment": {env_var: os.environ[env_var] for env_var in step.env_vars},
        }
        lambda_properties.update(format_lambda_properties(step.aws_lambda_properties))

        super().__init__(
            scope,
            f"{lambda_prefix}_{self.lambda_name}",
            code=lambda_.Code.from_asset(
                str(d.absolute()),
                exclude=["__pycache__"],
                bundling=core.BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        f'echo "{handler_file}" > /asset-output/{handler_name}.py && pip install -r {requirements_path} -t /asset-output {step_reqs_cmd} && cp -au . /asset-output/{d}',
                    ],
                ),
            ),
            handler=f"{handler_name}.handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            layers=[base_layer],
            **lambda_properties,
        )

        for permission in step.permissions.get(CloudProvider.aws, []):
            permission.aws_grant(scope, self)

    def get_handler_template_contents(self):
        template_file_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "templates",
            "handler.py.template",
        )
        with open(template_file_path, "r") as f:
            return f.read()
