from pyspark.sql.types import (
    ArrayType, 
    DoubleType, 
    LongType, 
    StringType,
    StructField, 
    StructType
)

class OrderSchema:

    def get(self) -> StructType:
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