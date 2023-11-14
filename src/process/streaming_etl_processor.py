from pyspark.sql import functions as F

class StreamingETLProcessor:
    def __init__(
        self, 
        spark,
        bucket_name,
        input_path, 
        output_path,
        process_name,
        process,
        schema
    ):
        self.spark = spark
        self.bucket_name = bucket_name
        self.input_path = input_path
        self.output_path = output_path
        self.checkpoint_location = '/opt/spark/app/checkpoint'
        self.process_name = process_name
        self.process = process
        self.schema = schema

    def __extract(self):
        self.raw_data = (
            self.spark.readStream.format("json")
            .schema(self.schema)
            .option('maxFilesPerTrigger', 1)
            .option('ignoreChanges', 'true')
            .option("multiline", 'true')
            .json(f'{self.bucket_name}/{self.input_path}')
        )
        return self

    def __parse(self, df, batch_id):
        if not df.isEmpty():
            df_raw = df.withColumn('dt_processamento', F.current_date())
            df_raw.write.mode('append').partitionBy(
                'dt_processamento'
            ).json(f'{self.bucket_name}/raw/{self.process_name}/')

            self.__transform(df)

    def __transform(self, df):
        df_transformed = self.process(
            spark=self.spark,
            bucket_name=self.bucket_name,
            raw_data=df,
            path_file=self.output_path,
            process_name=self.process_name
        ).run()

        df_transformed = (
            df_transformed.
            withColumn('dt_processamento', F.current_date())
        )
        
        self.__load(df_transformed)

    def __load(self, df):
        df.write.mode('overwrite').partitionBy(
            'dt_processamento'
        ).json(f'{self.bucket_name}/trusted/{self.process_name}/')
    
    def __write_streaming(self):
        query = (
            self.raw_data.writeStream
            .foreachBatch(self.__parse)
            .outputMode("append")
            .option('checkpointLocation', self.checkpoint_location)
            .start()
        )
        query.awaitTermination()

    def run(self):
        (
            self.__extract()
            .__write_streaming()
        )
