
from pyspark.sql import SparkSession

def ensure_meta_tables(spark: SparkSession):
    """
    Garante que o schema e a tabela de log existam.
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS meta")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS meta.run_log (
      run_ts TIMESTAMP,
      layer STRING,
      object_name STRING,
      status STRING,
      row_count BIGINT,
      details STRING
    ) USING DELTA
    """)

def log_run(
    spark: SparkSession,
    layer: str,
    object_name: str,
    status: str,
    row_count: int = 0,
    details: str = ""
):
    """
    Insere uma linha de log em meta.run_log.
    """
    ensure_meta_tables(spark)

    safe_details = (details or "").replace("'", "''")

    spark.sql(f"""
      INSERT INTO meta.run_log
      SELECT
        current_timestamp() as run_ts,
        '{layer}' as layer,
        '{object_name}' as object_name,
        '{status}' as status,
        {int(row_count)} as row_count,
        '{safe_details}' as details
    """)
