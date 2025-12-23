
from typing import Callable, Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import json

class LayerController:
    """Template para operações de leitura -> transformação -> escrita em Delta,
    com logging/monitoramento simples via meta.run_log.
    """

    def __init__(self, spark, layer: str, object_name: str, source: Optional[str] = None):
        self.spark = spark
        self.layer = layer
        self.object_name = object_name
        self.source = source
        self._ensure_meta()

    def _ensure_meta(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS meta")
        self.spark.sql("""
        CREATE TABLE IF NOT EXISTS meta.run_log (
          run_ts TIMESTAMP,
          layer STRING,
          object_name STRING,
          status STRING,
          rows BIGINT,
          details STRING
        ) USING DELTA
        """)

    def log(self, status: str, rows: Optional[int] = None, details: Optional[Dict[str, Any]] = None):
        details_str = json.dumps(details) if isinstance(details, dict) else (details or None)
        df = self.spark.createDataFrame(
            [(self.layer, self.object_name, status, rows, details_str)],
            "layer string, object_name string, status string, rows long, details string"
        ).withColumn("run_ts", F.current_timestamp())

        (df.select("run_ts","layer","object_name","status","rows","details")
           .write.mode("append").saveAsTable("meta.run_log"))

    def read_table(self, table: str) -> DataFrame:
        return self.spark.table(table)

    def write_table(self, df: DataFrame, table: str, mode: str = "overwrite") -> None:
        (df.write.format("delta").mode(mode).saveAsTable(table))

    def run(self,
            read_fn: Callable[[], DataFrame],
            transform_fn: Callable[[DataFrame], DataFrame],
            write_table: str,
            mode: str = "overwrite",
            extra_log: Optional[Dict[str, Any]] = None) -> None:
        try:
            df_in = read_fn()
            df_out = transform_fn(df_in)
            self.write_table(df_out, write_table, mode=mode)
            rows = df_out.count()
            self.log("SUCCESS", rows=rows, details={"source": self.source, **(extra_log or {})})
        except Exception as e:
            self.log("FAILED", rows=None, details={"error": repr(e), "source": self.source, **(extra_log or {})})
            raise
