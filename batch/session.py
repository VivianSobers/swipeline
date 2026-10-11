import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "swipeline-batch") -> SparkSession:
    """Builds and returns a SparkSession configured for Hudi COW tables and S3A storage."""
    master = os.environ.get("SPARK_MASTER", "local[1]")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "any")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "any")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    spark = builder.getOrCreate()
    return spark
