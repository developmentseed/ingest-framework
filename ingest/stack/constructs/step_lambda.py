import os
from pathlib import Path
from typing import Type
from aws_cdk import core, aws_lambda as lambda_, aws_s3 as s3


class StepLambda(lambda_.Function):
    from ingest.permissions import Permission
    from ingest.step import Step

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        step: Type[Step],
        code_dir: Path,
        default_requirements_path: Path,
        base_layer: lambda_.ILayerVersion,
        **kwargs,
    ):
        d = code_dir.relative_to(Path(os.path.curdir).resolve())

        if step.requirements_path:
            reqs = step.requirements_path.relative_to(code_dir)
        else:
            reqs = default_requirements_path.relative_to(code_dir)

        handler_file = self.get_handler_template_contents().format(
            handler_module=step.__module__, handler_class=step.__name__
        )
        self.lambda_name = step.__name__
        lambda_prefix = id[: 79 - len(self.lambda_name)]

        handler_name = "handler"

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
                        f'echo "{handler_file}" > /asset-output/{handler_name}.py && pip install -r {reqs} -t /asset-output && cp -au . /asset-output/{d}',
                    ],
                ),
            ),
            handler=f"{handler_name}.handler",
            timeout=core.Duration.minutes(1),
            runtime=lambda_.Runtime.PYTHON_3_9,
            layers=[base_layer],
        )

        for permission in step.permissions:
            self.grant_permission(permission)

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

    def grant_permission(self, permission: Permission):
        from ingest.permissions import S3Access

        if isinstance(permission, S3Access):
            bucket = s3.Bucket.from_bucket_name(self, "bucket", permission.bucket_name)
            for action in permission.actions:
                getattr(bucket, f"grant_{action}")(self)
