import os
from dotenv import load_dotenv
from enum import Enum
from importlib import import_module
from pathlib import Path


class CloudProvider(str, Enum):
    aws = "aws"
    local = "local"


def load_dotenv_file():
    env_path = os.getenv("ENV_FILE_PATH")
    if env_path:
        load_dotenv(Path(env_path).resolve())


def bootstrap(env: CloudProvider):
    load_dotenv_file()
    import_module(f".{env}", package=__name__)
