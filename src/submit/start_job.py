from pyspark.sql import SparkSession

from process import StreamingETLProcessor

class SparkSubmit:
        
    def run(
        self,
        bucket_name: str,
        input_path: str,
        output_path: str,
        process_name: str,
        process,
        schema
    ):
        
        spark = (
            SparkSession.builder.appName("StreamingETLProcessor")
            .config(
                'spark.hadoop.fs.s3a.impl', 
                'org.apache.hadoop.fs.s3a.S3AFileSystem'
            )
            .config("spark.hadoop.fs.s3a.access.key", 'dummy')
            .config("spark.hadoop.fs.s3a.secret.key", "dummy")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
            .getOrCreate()
        )

        StreamingETLProcessor(
            spark,
            bucket_name,
            input_path,
            output_path,
            process_name,
            process,
            schema
        ).run()