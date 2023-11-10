from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType, 
    DoubleType, 
    LongType, 
    StringType,
    StructField, 
    StructType
)

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from sys import argv

class Order:
    def __init__(self, columns: list):
        self.columns = columns

class Product:
    def __init__(self, columns: list):
        self.columns = columns

class Seller:
    def __init__(self, columns: list):
        self.columns = columns

class Delivery:
    def __init__(self, columns: list):
        self.columns = columns


class StreamingETLProcessor:
    def __init__(self, spark, input_path, output_path, checkpoint_location):
        self.spark = spark
        self.input_path = input_path
        self.output_path = output_path
        self.checkpoint_location = checkpoint_location

    def extract(self):
        self.raw_data = (
            self.spark.readStream.format("json")
            .schema(self.get_schema())
            .option('maxFilesPerTrigger', 1)
            .option('ignoreChanges', 'true')
            .option("multiline", 'true')
            .json(self.input_path)
        )

    def transform(self):

        self.pedidos_df = self.raw_data.select(
            col("txt_detl_idt_pedi_pgto"),
            col("cod_idef_clie"),
            col("dat_hor_tran"),
            col("list_item_pedi"),
            col("stat_pedi")
        )

        window_spec = Window.partitionBy('pedido_id').orderBy(col('data_transacao').desc())

        # self.final_df = self.pedidos_df.withColumn(
        #     'rank', F.row_number().over(window_spec)
        # ).filter(col('rank') == 1
        # ).drop('rank')


    def merge_data(self, df, batch_id):
        df.write.format("org.apache.spark.sql.cassandra").options(table="pedidos", keyspace="itau_shop").mode('append').save()

    def load(self):
        
        query = (
            self.pedidos_df.writeStream
            .foreachBatch(self.merge_data)
            .outputMode("append")
            .option('checkpointLocation', self.checkpoint_location)
            .start()
        )
        query.awaitTermination()

    def get_schema(self):
        
        return StructType([
            StructField('cod_idef_clie', StringType(), True), 
            StructField('dat_atui', StringType(), True), 
            StructField('dat_hor_tran', StringType(), True), 
            StructField('list_envo', ArrayType(
                StructType([
                    StructField('cod_tipo_entg', StringType(), True), 
                    StructField('cod_venr', StringType(), True), 
                    StructField('dat_emis_cpvt', StringType(), True), 
                    StructField('list_item_envo', ArrayType(StructType([
                        StructField('cod_prod', StringType(), True), 
                        StructField('cod_prod_venr', StringType(), True), 
                        StructField('qtd_prod', LongType(), True), 
                        StructField('stat_entg', StringType(), True)
                ]), True), True), 
                StructField('nom_stat_entg', StringType(), True)
            ]), True), True), 
            StructField('list_item_pedi', ArrayType(StructType([
                StructField('cod_ofrt_ineo_requ', StringType(), True), 
                StructField('cod_vrne_prod', StringType(), True), 
                StructField('idt_venr', StringType(), True), 
                StructField('idt_vrne_venr', StringType(), True), 
                StructField('nom_item', StringType(), True), 
                StructField('nom_venr', StringType(), True), 
                StructField('qtd_item_pedi', LongType(), True), 
                StructField('vlr_oril', DoubleType(), True), 
                StructField('vlr_prod', DoubleType(), True), 
                StructField('vlr_prod_ofrt_desc', DoubleType(), True)
            ]), True), True), 
            StructField('stat_pedi', StringType(), True), 
            StructField('stat_pgto', StringType(), True), 
            StructField('txt_detl_idt_pedi_pgto', StringType(), True)
        ])

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("StreamingETLProcessor")
        .config(
            "spark.jars.packages", 
            "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,com.github.jnr:jnr-posix:3.0.52"
        )
        .config("spark.cassandra.connection.host", argv[1])
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
        .getOrCreate()
    )
    input_path = "/opt/spark/app/input/*.json"
    output_path = "/opt/spark/app/output"
    checkpoint_location = '/opt/spark/app/checkpoint'

    streaming_etl_processor = StreamingETLProcessor(
        spark, 
        input_path, 
        output_path, 
        checkpoint_location
    )
    streaming_etl_processor.extract()
    streaming_etl_processor.transform()
    streaming_etl_processor.load()
