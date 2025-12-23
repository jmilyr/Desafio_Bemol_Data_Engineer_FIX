
import sys, json
sys.path.append(".")
from utils.dq_helpers import dq_profile_customers, print_metrics
from utils.run_log import log_run

TBL = "bronze.customers"

df = spark.table(TBL)
metrics = dq_profile_customers(df)
print_metrics(metrics)

if metrics["rows"] == 0:
    log_run("bronze", TBL, "FAILED", 0, "rows=0")
    raise ValueError("Bronze vazio (rows=0)")

if metrics["null_customer_id"] > 0:
    log_run("bronze", TBL, "FAILED", metrics["rows"], "null_customer_id>0")
    raise ValueError("Contrato violado: customer_id nulo")

log_run("bronze", TBL, "SUCCESS", metrics["rows"], json.dumps(metrics))
