from interface import StreamingETLInterface
from schemas import OrderSchema

class Order(StreamingETLInterface):
    def __init__(self, spark, raw_data, output_path:str):
        self.spark = spark
        self.output_path = output_path
        self.raw_data = raw_data

    def __check_file_exists(self):
        sc = self.spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create(self.output_path),
            sc._jsc.hadoopConfiguration(),
        )
        return fs.exists(
            sc._jvm.org.apache.hadoop.fs.Path(self.output_path)
        )

    def extract(self):
        self.orders = self.spark.read.parquet(self.output_path)
        return self
        
    def transform(self):
        self.order_join = self.orders.select(
            "txt_detl_idt_pedi_pgto",
            "cod_idef_clie",
            "dat_hor_tran",
            "stat_pgto",
            "stat_pedi",
            "dat_atui"
        ).union(self.raw_data).dropDuplicates(
            ["txt_detl_idt_pedi_pgto", "dat_atui"]
        )

        self.products = self.orders.selectExpr(
            "txt_detl_idt_pedi_pgto",
            "list_item_pedi.nom_item as nom_item",
            "list_item_pedi.cod_ofrt_ineo_requ as cod_ofrt_ineo_requ",
            "list_item_pedi.vlr_oril as vlr_oril",
            "list_item_pedi.vlr_prod as vlr_prod",
            "list_item_pedi.vlr_prod_ofrt_desc as vlr_prod_ofrt_desc",
            "list_item_pedi.qtd_item_pedi as qtd_item_pedi",
            "list_item_pedi.idt_venr as idt_venr",
            "list_item_pedi.idt_vrne_venr as idt_vrne_venr"
        ).union(self.raw_data).dropDuplicates(["cod_ofrt_ineo_requ"])

        self.sellers = self.orders.selectExpr(
            "list_item_pedi.idt_venr as idt_venr",
            "list_item_pedi.nom_venr as nom_venr",
            "list_item_pedi.idt_vrne_venr as idt_vrne_venr",
            "list_item_pedi.cod_vrne_prod as cod_vrne_prod",
        ).union(self.raw_data).dropDuplicates(["idt_venr", "idt_vrne_venr"])

        self.deliveries = self.orders.selectExpr(
            "txt_detl_idt_pedi_pgto",
            "list_envo.cod_tipo_entg as cod_tipo_entg",
            "list_envo.cod_venr as cod_venr",
            "list_envo.nom_stat_entg as nom_stat_entg",
            "list_item_envo.qtd_prod as qtd_prod",
            "list_item_envo.cod_prod_venr as cod_prod_venr",
            "list_item_envo.cod_prod as cod_prod",
            "list_item_envo.stat_entg as stat_entg",
        ).union(self.raw_data).dropDuplicates(["txt_detl_idt_pedi_pgto"])
        
        return self

    def load(self):
        self.order_join.write.mode('overwrite').parquet(f'{self.output_path}/orders/')
        self.products.write.mode('overwrite').parquet(f'{self.output_path}/products/')
        self.sellers.write.mode('overwrite').parquet(f'{self.output_path}/sellers/')
        self.deliveries.write.mode('overwrite').parquet(f'{self.output_path}/deliveries/')

    def run(self):
        if self.__check_file_exists():
            self.extract().transform().load()