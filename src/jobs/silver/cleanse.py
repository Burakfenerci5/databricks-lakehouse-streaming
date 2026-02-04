from pyspark.sql.functions import col
from src.shared_utils import get_spark_session

def run_silver_curation(bronze_path, silver_path):
    spark = get_spark_session("SilverCuration")

    # Read from Bronze Delta Table
    bronze_df = spark.readStream \
        .format("delta") \
        .load(bronze_path)

    # TRANSFORMATION:
    # 1. Filter out garbage data (Signal strength must be positive)
    # 2. Deduplicate based on device_id and timestamp (within 10 min window)
    clean_df = bronze_df \
        .filter(col("signal_strength") > 0) \
        .withWatermark("timestamp", "10 minutes") \
        .dropDuplicates(["device_id", "timestamp"])

    # MERGE/UPSERT pattern is common here, but for streaming we append cleaned data
    query = clean_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{silver_path}/_checkpoints") \
        .start(silver_path)

    query.awaitTermination()