from abc import abstractmethod
from pydantic import BaseModel


class Permission(BaseModel):
    pass

    @abstractmethod
    def aws_grant(self, scope, func):
        ...
