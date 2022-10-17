from pydantic import BaseModel
import os


class UnsetEnvVar(Exception):
    ...


class EnvVar(BaseModel):
    """
    A dependency on an environment variable.
    """

    name: str

    @property
    def value(self):
        try:
            return os.environ[self.name]
        except KeyError:
            raise UnsetEnvVar(
                f"The following environment variable was unset: {self.name}"
            )
