import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils.spark_session import create_spark_session

spark = create_spark_session()

SILVER_RECON_PATH = "C:/Users/developer/Desktop/Git_project2/data/silver/reconciliation_base"

df = spark.read.format("parquet").load(SILVER_RECON_PATH)

print(f"\n{'='*70}")
print(f"  Silver Table : reconciliation_base")
print(f"  Total Rows   : {df.count()}")
print(f"  Total Columns: {len(df.columns)}")
print(f"{'='*70}")

print("\nSchema:")
df.printSchema()

print("\nData:")
df.show(100, truncate=False)

spark.stop()
