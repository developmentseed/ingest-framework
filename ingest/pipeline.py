from pathlib import Path
from typing import Any, Sequence
from ingest.providers import CloudProvider, bootstrap
import subprocess
import sys

from ingest.step import Collector, Step
from ingest.trigger import Trigger


class Pipeline:
    """
    This implementation does not support statically checking the input/output
    types of each step. That validation can only happen at runtime.

    But it has some significant benefits:
    1. Supports an arbitrary number of Steps.
    2. Does not require us to define the input/output types of each step twice.
    """

    uuid: str
    name: str
    steps: Sequence[Step]
    trigger: Trigger

    def __init__(self, name: str, trigger: Trigger, steps: Sequence[Step]):
        self.uuid = "testuuid"  # uuid4()
        self.name = name
        self.trigger = trigger
        self.steps = steps
        self.validate()

    def run(self, input):
        bootstrap(CloudProvider.local)
        self.install_requirements()
        for step in self.steps:
            if isinstance(step, Collector):
                step.collect_input(input)
                if step.ready():
                    input = step(input=step.fetch_batch())
                else:
                    return input
            else:
                input = step(input=input)
        return input

    def validate(self):
        """Ensure that each step passes the correct data type
        to the following step."""
        if self.trigger.output_type != self.steps[0].get_input():
            raise TypeError(
                f"Output of trigger ({self.trigger.output_type} is not equal to input of first step ({self.steps[0].get_input()})"
            )

        # check output of each step against input of the following step
        i = 0
        while i < len(self.steps) - 1:
            output_type = self.steps[i].get_output()
            input_type = self.steps[i + 1].get_input()
            if output_type != input_type:
                raise TypeError(
                    f"Output of step {i} ({output_type} is not equal to input of step {i+1} ({input_type})"
                )
            i += 1

    def install_requirements(self):
        reqs = []
        for step in self.steps:
            reqs.extend(step.requirements)
        subprocess.check_call([sys.executable, "-m", "pip", "install", *reqs])

    def create_stack(self, app: Any, code_dir: Path, requirements_path: Path):
        from ingest.stack.pipeline_stack import PipelineStack

        return PipelineStack(
            app,
            id=f"PipelineStack-{self.uuid}",
            pipeline=self,
            steps=self.steps,
            code_dir=code_dir,
            requirements_path=requirements_path,
        )

    @property
    def resource_name(self):
        return self.name.replace(" ", "_")
