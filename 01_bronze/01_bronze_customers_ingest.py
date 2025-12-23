
import sys
sys.path.append(".")

from pyspark.sql import functions as F
from utils.data_contract_customers import normalize_columns, assert_has_columns, CONTRACT_COLUMNS
from utils.layer_controller import LayerController

spark.sql("CREATE DATABASE IF NOT EXISTS landing")
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

SOURCE_TABLE = "default.olist_customers_dataset"
TARGET_TABLE = "bronze.customers"

ctrl = LayerController(spark, layer="bronze", object_name=TARGET_TABLE, source=SOURCE_TABLE)

def read_fn():
    return spark.table(SOURCE_TABLE)

def transform_fn(df):
    df = normalize_columns(df)
    assert_has_columns(df, CONTRACT_COLUMNS)

    df = (
        df
        .dropna(subset=["customer_id", "customer_unique_id"])
        .withColumn("customer_state", F.upper(F.col("customer_state")))
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    return df

ctrl.run(read_fn=read_fn, transform_fn=transform_fn, write_table=TARGET_TABLE, mode="overwrite",
         extra_log={"note":"bronze ingest + basic cleaning"} )

display(spark.table(TARGET_TABLE).limit(10))
