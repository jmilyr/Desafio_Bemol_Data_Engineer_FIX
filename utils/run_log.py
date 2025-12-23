
from pyspark.sql import functions as F

def ensure_meta_tables():
    spark.sql("CREATE DATABASE IF NOT EXISTS meta")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS meta.run_log (
      run_ts TIMESTAMP,
      layer STRING,
      object_name STRING,
      status STRING,
      rows BIGINT,
      details STRING
    ) USING DELTA
    """)

def log_run(layer: str, object_name: str, status: str, rows: int = None, details: str = None):
    ensure_meta_tables()
    df = spark.createDataFrame([(
        None, layer, object_name, status, rows, details
    )], "run_ts timestamp, layer string, object_name string, status string, rows long, details string")         .withColumn("run_ts", F.current_timestamp())
    df.write.mode("append").saveAsTable("meta.run_log")
