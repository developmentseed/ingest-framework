from pathlib import Path
from typing import List
from uuid import uuid4
from pydantic import UUID4

from ingest.pipeline import Pipeline


class IngestApp:
    uuid: UUID4
    name: str
    pipelines: List[Pipeline]
    code_dir: Path
    requirements_path: Path

    def __init__(
        self,
        name: str,
        code_dir: Path,
        requirements_path: Path,
        pipelines: List[Pipeline] = [],
    ):
        self.id = uuid4()
        self.name = name
        self.pipelines = pipelines
        self.code_dir = code_dir
        self.requirements_path = requirements_path

    def synth(self):
        """Synthesize the CDK App"""
        from aws_cdk import core

        app = core.App()
        for pipeline in self.pipelines:
            pipeline.create_stack(
                app, code_dir=self.code_dir, requirements_path=self.requirements_path
            )
        return app.synth()
