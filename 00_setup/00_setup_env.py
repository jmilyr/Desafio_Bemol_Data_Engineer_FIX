
spark.sql("CREATE DATABASE IF NOT EXISTS landing")
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("CREATE DATABASE IF NOT EXISTS meta")
print("✅ Databases criados: landing/bronze/silver/gold/meta")
