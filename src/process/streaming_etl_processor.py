from pyspark.sql import functions as F

class StreamingETLProcessor:
    def __init__(
        self, 
        spark,
        bucket_name,
        input_path, 
        output_path, 
        checkpoint_location, 
        process_name
    ):
        self.spark = spark
        self.bucket_name = bucket_name
        self.input_path = input_path
        self.output_path = output_path
        self.checkpoint_location = checkpoint_location
        self.process_name = process_name

    def extract(self, schema):
        self.raw_data = (
            self.spark.readStream.format("json")
            .schema(schema)
            .option('maxFilesPerTrigger', 1)
            .option('ignoreChanges', 'true')
            .option("multiline", 'true')
            .json(f'{self.bucket_name}/{self.input_path}')
        )

    def transform(self, process):
        process(
            spark=self.spark,
            raw_data=self.raw_data,
            output_path=f'{self.bucket_name}/{self.output_path}'
        ).run()

    def load_data(self, df, batch_id):
        df = df.withColumn('dt_processamento', F.current_date())
        
        df.write.mode('append').partitionBy('dt_processamento').parquet(
            f'{self.bucket_name}/raw/{self.process_name}/'
        )

    def load(self):
        query = (
            self.raw_data.writeStream
            .foreachBatch(self.load_data)
            .outputMode("append")
            .option('checkpointLocation', self.checkpoint_location)
            .start()
        )
        query.awaitTermination()
