from pyspark.sql import SparkSession

from models import Order
from process import StreamingETLProcessor
from schemas import OrderSchema

if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("StreamingETLProcessor")
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config("spark.hadoop.fs.s3a.access.key", 'dummy')
        .config("spark.hadoop.fs.s3a.secret.key", "dummy")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
        .getOrCreate()
    )

    bucket_name = 's3a://itau-shop'
    input_path = 'input/*.json'
    output_path = 'analytics'
    process_name = 'marketing_place'

    StreamingETLProcessor(
        spark,
        bucket_name,
        input_path,
        output_path,
        process_name,
        Order,
        OrderSchema().get()
    ).run()
