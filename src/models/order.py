from interface import ETLInterface

from pyspark.sql.window import Window

from pyspark.sql import functions as F

class Order(ETLInterface):
    def __init__(self, spark, bucket_name, raw_data, path_file, process_name):
        self.spark = spark
        self.bucket_name = bucket_name
        self.path_file = path_file
        self.raw_data = raw_data
        self.process_name = process_name

    def __check_file_exists(self):
        path_file = f'{self.bucket_name}/{self.path_file}'
        sc = self.spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create(path_file),
            sc._jsc.hadoopConfiguration(),
        )
        return fs.exists(
            sc._jvm.org.apache.hadoop.fs.Path(path_file)
        )

    def extract(self):
        self.df_orders = self.spark.read.parquet(
            f'{self.bucket_name}/{self.path_file}/orders/*.parquet'
        )
        self.df_products = self.spark.read.parquet(
            f'{self.bucket_name}/{self.path_file}/products/*.parquet'
        )
        self.df_sellers = self.spark.read.parquet(
            f'{self.bucket_name}/{self.path_file}/sellers/*.parquet'
        )
        self.df_deliveries = self.spark.read.parquet(
            f'{self.bucket_name}/{self.path_file}/deliveries/*.parquet'
        )
        return self

    def __first_ingestion(self):

        self.orders = self.raw_data.select(
            "txt_detl_idt_pedi_pgto",
            "cod_idef_clie",
            "dat_hor_tran",
            "stat_pgto",
            "stat_pedi",
            "dat_atui",
        )

        self.products = self.raw_data.selectExpr(
            "txt_detl_idt_pedi_pgto",
            "EXPLODE(list_item_pedi) AS list_item_pedi",
        ).select(
            "txt_detl_idt_pedi_pgto",
            "list_item_pedi.nom_item",
            "list_item_pedi.cod_ofrt_ineo_requ",
            "list_item_pedi.vlr_oril",
            "list_item_pedi.vlr_prod",
            "list_item_pedi.vlr_prod_ofrt_desc",
            "list_item_pedi.qtd_item_pedi",
            "list_item_pedi.idt_venr",
            "list_item_pedi.nom_venr",
            "list_item_pedi.idt_vrne_venr",
            "list_item_pedi.cod_vrne_prod",
        )

        self.sellers = self.raw_data.selectExpr(
            "EXPLODE(list_item_pedi) AS list_item_pedi",
        ).select(
            "list_item_pedi.idt_venr",
            "list_item_pedi.nom_venr",
            "list_item_pedi.idt_vrne_venr",
            "list_item_pedi.cod_vrne_prod",
        )

        self.deliveries = self.raw_data.select(
            "txt_detl_idt_pedi_pgto",
            "list_envo",
        )

        self.df = self.raw_data

        return self

    def transform(self):
        self.orders = self.df_orders.select(
            "txt_detl_idt_pedi_pgto",
            "cod_idef_clie",
            "dat_hor_tran",
            "stat_pgto",
            "stat_pedi",
            "dat_atui"
        ).union(
            self.raw_data.select(
                "txt_detl_idt_pedi_pgto",
                "cod_idef_clie",
                "dat_hor_tran",
                "stat_pgto",
                "stat_pedi",
                "dat_atui"   
            )
        ).dropDuplicates(["txt_detl_idt_pedi_pgto", "dat_atui"])

        self.products = self.df_products.select(
            "txt_detl_idt_pedi_pgto",
            "nom_item",
            "cod_ofrt_ineo_requ",
            "vlr_oril",
            "vlr_prod",
            "vlr_prod_ofrt_desc",
            "qtd_item_pedi",
            "idt_venr",
            "nom_venr",
            "idt_vrne_venr",
            "cod_vrne_prod"
        ).union(
            self.raw_data.selectExpr(
                "txt_detl_idt_pedi_pgto", 
                "EXPLODE(list_item_pedi) AS list_item_pedi"
        ).select(
            "txt_detl_idt_pedi_pgto",
            "list_item_pedi.nom_item",
            "list_item_pedi.cod_ofrt_ineo_requ",
            "list_item_pedi.vlr_oril",
            "list_item_pedi.vlr_prod",
            "list_item_pedi.vlr_prod_ofrt_desc",
            "list_item_pedi.qtd_item_pedi",
            "list_item_pedi.idt_venr",
            "list_item_pedi.nom_venr",
            "list_item_pedi.idt_vrne_venr",
            "list_item_pedi.cod_vrne_prod"
        )
        ).distinct()

        self.sellers = self.df_sellers.select(
            "idt_venr",
            "nom_venr",
            "idt_vrne_venr",
            "cod_vrne_prod",
        ).union(
            self.raw_data.selectExpr(
                "EXPLODE(list_item_pedi) AS list_item_pedi"
        ).selectExpr(
                "list_item_pedi.idt_venr",
                "list_item_pedi.nom_venr",
                "list_item_pedi.idt_vrne_venr",
                "list_item_pedi.cod_vrne_prod",
            )
        ).distinct()
        
        self.deliveries = self.df_deliveries.select(
            "txt_detl_idt_pedi_pgto",
            "list_envo"
        ).union(
            self.raw_data.select(
                "txt_detl_idt_pedi_pgto",
                "list_envo"              
            )
        ).distinct()

        window_spec = (
            Window.partitionBy("txt_detl_idt_pedi_pgto")
            .orderBy(F.desc("dat_atui"))
        )

        df_trusted = self.spark.read.option("multiline", "true").json(
            f'{self.bucket_name}/trusted/{self.process_name}/*/*.json'
        )
        df = self.raw_data.join(
            df_trusted, on=['txt_detl_idt_pedi_pgto'], how='left'
        )

        df = df.withColumn("version", F.row_number().over(window_spec))
        self.df = df.filter(F.col("version") == 1)

        return self

    def load(self):
        self.orders.write.mode('overwrite').parquet(
            f'{self.bucket_name}/{self.path_file}/orders/'
        )
        self.products.write.mode('overwrite').parquet(
            f'{self.bucket_name}/{self.path_file}/products/'
        )
        self.sellers.write.mode('overwrite').parquet(
            f'{self.bucket_name}/{self.path_file}/sellers/'
        )
        self.deliveries.write.mode('overwrite').parquet(
            f'{self.bucket_name}/{self.path_file}/deliveries/'
        )

    def run(self):
        if self.__check_file_exists():
            self.extract().transform().load()
        else:
            self.__first_ingestion().load()

        return self.df