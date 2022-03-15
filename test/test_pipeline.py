import pytest
from ingest.pipeline import Pipeline
from test.data_models import S3ToStac, StacToS3


class TestPipeline:
    def test_creation(self):
        """Pipelines can be created"""
        pipe = Pipeline("TestCreate", steps=[S3ToStac, StacToS3])
        assert pipe.name == "TestCreate"
        assert pipe.steps == [S3ToStac, StacToS3]

    def test_validation(self):
        """Each step in a Pipeline must pass the expected data type
        to the following step."""
        with pytest.raises(TypeError):
            Pipeline("TestCreate", steps=[S3ToStac, S3ToStac])
