
import sys
sys.path.append(".")

from pyspark.sql import functions as F
from utils.layer_controller import LayerController

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

SRC = "bronze.customers"
TGT = "silver.dim_customer"

ctrl = LayerController(spark, layer="silver", object_name=TGT, source=SRC)

def read_fn():
    return spark.table(SRC)

def transform_fn(df):
   
    df2 = (
        df
        .dropDuplicates(["customer_id"])
        .withColumn("customer_city", F.initcap(F.col("customer_city")))
        .withColumn("customer_state", F.upper(F.col("customer_state")))
    )
    return df2

ctrl.run(read_fn=read_fn, transform_fn=transform_fn, write_table=TGT, mode="overwrite",
         extra_log={"note":"silver clean + dedup"} )

display(spark.table(TGT).limit(10))
