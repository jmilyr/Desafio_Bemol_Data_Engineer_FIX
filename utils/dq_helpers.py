
from pyspark.sql import functions as F

def dq_profile_customers(df):
  
    total = df.count()
    null_customer_id = df.filter(F.col("customer_id").isNull()).count()
    null_unique = df.filter(F.col("customer_unique_id").isNull()).count()
    dup_customer_id = (
        df.groupBy("customer_id").count()
          .filter(F.col("count") > 1).count()
    )
    invalid_state = df.filter((F.col("customer_state").isNotNull()) & (F.length("customer_state") != 2)).count()

    return {
        "rows": total,
        "null_customer_id": null_customer_id,
        "null_customer_unique_id": null_unique,
        "duplicate_customer_id": dup_customer_id,
        "invalid_state_len": invalid_state
    }

def print_metrics(metrics: dict):
    for k, v in metrics.items():
        print(f"{k}: {v}")
