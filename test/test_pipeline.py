import pytest
from ingest.pipeline import Pipeline
from ingest.trigger import S3ObjectCreated, S3Filter
from test.data_models import s3_to_stac, stac_to_s3


class TestPipeline:
    def test_creation(self):
        """Pipelines can be created"""
        pipe = Pipeline(
            "TestCreate",
            trigger=S3ObjectCreated(
                bucket_name="fakebucket",
                object_filter=S3Filter(prefix="inbox", suffix=".json"),
            ),
            steps=[s3_to_stac, stac_to_s3],
        )
        assert pipe.name == "TestCreate"
        assert pipe.steps == [s3_to_stac, stac_to_s3]

    def test_step_output_validation(self):
        """Each step in a Pipeline must pass the expected data type
        to the following step."""
        with pytest.raises(TypeError):
            Pipeline(
                "TestCreate",
                trigger=S3ObjectCreated(
                    bucket_name="fakebucket",
                    object_filter=S3Filter(prefix="inbox", suffix=".json"),
                ),
                steps=[s3_to_stac, s3_to_stac],
            )

    def test_trigger_output_validation(self):
        """The pipeline trigger must produce the data type that the first
        step expects to receives."""
        with pytest.raises(TypeError):
            Pipeline(
                "TestCreate",
                trigger=S3ObjectCreated(
                    bucket_name="fakebucket",
                    object_filter=S3Filter(prefix="inbox", suffix=".json"),
                ),
                steps=[stac_to_s3, s3_to_stac],
            )
