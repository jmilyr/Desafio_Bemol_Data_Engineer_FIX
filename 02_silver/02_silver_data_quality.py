
import sys, json
sys.path.append(".")
from pyspark.sql import functions as F
from utils.run_log import log_run

TBL = "silver.dim_customer"
df = spark.table(TBL)

rows = df.count()
dup = df.groupBy("customer_id").count().filter(F.col("count") > 1).count()
nulls = df.filter(F.col("customer_id").isNull() | F.col("customer_unique_id").isNull()).count()

metrics = {"rows": rows, "duplicate_customer_id": dup, "null_key_rows": nulls}
print(metrics)

if rows == 0 or dup > 0 or nulls > 0:
    log_run("silver", TBL, "FAILED", rows, json.dumps(metrics))
    raise ValueError(f"Silver DQ failed: {metrics}")

log_run("silver", TBL, "SUCCESS", rows, json.dumps(metrics))
