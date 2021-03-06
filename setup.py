"""ingest: a framework for data ingestion pipelines"""

from setuptools import find_packages, setup

with open("README.md") as f:
    desc = f.read()

with open("VERSION") as version_file:
    version = version_file.read().strip()

# TODO: update stac-fastapi dependencies to use pypi packages when this PR
# https://github.com/stac-utils/stac-fastapi/pull/308 is included in the next release.
# (Anything > 2.2.0)
install_requires = [
    "pydantic==1.9.0",
]

extra_reqs = {
    "dev": [
        "black==22.1.0",
        "mypy==0.931",
        "mypy-extensions==0.4.3",
        "pytest==7.1.0",
        "boto3==1.21.20",
        "boto3-stubs[stepfunctions]",
    ],
    "cdk": [
        "aws-cdk.core>=1.148.0",
        "aws-cdk.aws-ec2>=1.148.0",
        "aws-cdk.aws-s3>=1.148.0",
        "aws-cdk.aws-lambda-event-sources>=1.148.0",
        "aws-cdk.aws-iam>=1.148.0",
        "aws-cdk.aws-lambda>=1.148.0",
        "aws-cdk.aws-rds>=1.148.0",
        "aws-cdk.aws-sqs>=1.148.0",
        "aws-cdk.aws-stepfunctions>=1.148.0",
        "aws-cdk.aws-stepfunctions-tasks>=1.148.0",
    ],
}


setup(
    name="ingest",
    description=("A data ingestion framework"),
    long_description=desc,
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="data pipeline",
    author="Edward Keeble",
    author_email="edward@developmentseed.org",
    url="https://github.com/edkeeble/ingest",
    license="",
    package_data={
        "ingest": ["py.typed"],
    },
    packages=find_packages(
        exclude=["alembic", "tests", "scripts", "examples", "deps", "cdk.out", "build"],
    ),
    zip_safe=False,
    include_package_data=True,
    install_requires=install_requires,
    tests_require=extra_reqs["dev"],
    extras_require=extra_reqs,
    # scripts=["scripts/api", "scripts/format", "scripts/lint", "scripts/typecheck"],
    version=version,
)
