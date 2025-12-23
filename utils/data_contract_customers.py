
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


CUSTOMERS_CONTRACT = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

CONTRACT_COLUMNS = [f.name for f in CUSTOMERS_CONTRACT.fields]

def normalize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())
    return df

def assert_has_columns(df, required_cols):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return True
