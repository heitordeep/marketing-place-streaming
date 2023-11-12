from pyspark.sql import SparkSession

from process import StreamingETLProcessor
from schemas import OrderSchema
from models import Order
from sys import argv


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("StreamingETLProcessor")
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config("spark.hadoop.fs.s3a.access.key", 'dummy')
        .config("spark.hadoop.fs.s3a.secret.key", "dummy")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{argv[1]}:4566")
        .getOrCreate()
    )
    bucket_name = 's3a://itau-shop'
    input_path = 'input/*.json'
    output_path = 'output'
    checkpoint_location = '/opt/spark/app/checkpoint'
    process_name = 'marketing_place'

    streaming_etl_processor = StreamingETLProcessor(
        spark,
        bucket_name,
        input_path,
        output_path,
        checkpoint_location,
        process_name
    )
    streaming_etl_processor.extract(schema=OrderSchema().get())
    streaming_etl_processor.transform(Order)
    streaming_etl_processor.load()
