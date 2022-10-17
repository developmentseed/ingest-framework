import os
from pathlib import Path
from typing import Sequence
from uuid import uuid4
from pydantic import UUID4
from ingest.pipeline import Pipeline


class IngestApp:
    uuid: UUID4
    name: str
    pipelines: Sequence[Pipeline]
    code_dir: Path
    requirements_path: Path

    def __init__(
        self,
        name: str,
        code_dir: Path,
        requirements_path: Path,
        pipelines: Sequence[Pipeline] = [],
    ):
        self.id = uuid4()
        self.name = name
        self.pipelines = pipelines
        self.code_dir = code_dir
        self.requirements_path = requirements_path

    def synth(self):
        """Synthesize the CDK App"""
        from aws_cdk import App, Tags

        app = App()
        Tags.of(app).add("Project", os.environ.get("PROJECT_NAME"))
        Tags.of(app).add("Owner", os.environ.get("PROJECT_OWNER"))
        for pipeline in self.pipelines:
            pipeline.create_stack(
                app, code_dir=self.code_dir, requirements_path=self.requirements_path
            )
        return app.synth()
