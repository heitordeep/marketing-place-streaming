from pyspark.sql import functions as F
from pyspark.sql.window import Window

from interface import ETLInterface


class Order(ETLInterface):
    def __init__(self, spark, bucket_name, path_file, process_name):
        self.spark = spark
        self.bucket_name = bucket_name
        self.path_file = path_file
        self.process_name = process_name

    def extract(self):
        self.raw_orders = self.spark.read.option('multiline', 'true').json(
            f'{self.bucket_name}/raw/{self.process_name}/*.json'
        )
        return self

    def transform(self):
        self.orders = self.raw_orders.select(
            "txt_detl_idt_pedi_pgto",
            "cod_idef_clie",
            "dat_hor_tran",
            "stat_pgto",
            "stat_pedi",
            "dat_atui"
        )

        self.products = self.raw_orders.selectExpr(
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
        ).distinct()

        self.sellers = self.raw_orders.selectExpr(
            "EXPLODE(list_item_pedi) AS list_item_pedi",
        ).selectExpr(
            "list_item_pedi.idt_venr",
            "list_item_pedi.nom_venr",
            "list_item_pedi.idt_vrne_venr",
            "list_item_pedi.cod_vrne_prod",
        ).distinct()
        
        self.shipments = self.raw_orders.select(
            "txt_detl_idt_pedi_pgto",
            "list_envo",
            "dat_atui"
        )

        window_spec = (
            Window.partitionBy("txt_detl_idt_pedi_pgto")
            .orderBy(F.desc("dat_atui"))
        )

        self.orders = self.orders.withColumn(
            "version", 
            F.row_number().over(window_spec)
        ).filter(F.col("version") == 1).drop("version")

        self.shipments = self.shipments.withColumn(
            "version", 
            F.row_number().over(window_spec)
        ).filter(F.col("version") == 1).drop("version")

        self.trusted = self.products.alias('products').join(
            self.sellers.alias('sellers'), on='idt_venr', how='inner'
        ).join(
            self.orders.alias('orders'), on='txt_detl_idt_pedi_pgto', how='inner'
        ).join(
            self.shipments.alias('shipments'), on='txt_detl_idt_pedi_pgto', how='inner'
        ).select(
            'orders.txt_detl_idt_pedi_pgto',
            'orders.cod_idef_clie',
            'orders.dat_hor_tran',
            'products.nom_item',
            'products.cod_ofrt_ineo_requ',
            'products.vlr_oril',
            'products.vlr_prod',
            'products.vlr_prod_ofrt_desc',
            'products.qtd_item_pedi',
            'products.idt_vrne_venr',
            'products.idt_venr',
            'products.nom_venr',
            'products.cod_vrne_prod',
            'shipments.list_envo',
            'orders.stat_pgto',
            'orders.stat_pedi',
            'orders.dat_atui'
        ).distinct().withColumn('dt_processamento', F.current_date())

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
        self.shipments.write.mode('overwrite').parquet(
            f'{self.bucket_name}/{self.path_file}/shipments/'
        )

    def run(self):
        self.extract().transform().load()
        return self.trusted