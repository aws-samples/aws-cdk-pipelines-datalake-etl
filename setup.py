# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import setuptools


with open("README.md") as fp:
    long_description = fp.read()

setuptools.setup(
    name="aws_cdk_pipelines_blog_datalake_etl",
    version="0.0.1",
    description="A CDK Python app for deploying ETL jobs that utilize a Data Lake in AWS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Zahid Muhammad Ali <zhidli@amazon.com>, Isaiah Grant <igrant@2ndwatch.com>, Ravi Itha <itharav@amazon.com>",
    packages=setuptools.find_packages(),
    install_requires=[
        "aws-cdk.core~=1.110.0",
    ],
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
