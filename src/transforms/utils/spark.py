from __future__ import annotations
from typing import Dict, Optional
from pyspark.sql import SparkSession


def build_spark(
    app_name: str,
    *,
    use_s3a: bool = True,
    extra_confs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Standardized SparkSession:
      - dynamic partition overwrite
      - S3A filesystem + default AWS creds chain (OIDC / instance / env / ~/.aws)
      - Optional extra configs per job
    """
    builder = SparkSession.builder.appName(app_name).config(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )

    if use_s3a:
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
        )

    if extra_confs:
        for k, v in extra_confs.items():
            builder = builder.config(k, v)

    return builder.getOrCreate()
