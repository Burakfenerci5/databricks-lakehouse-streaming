from pyspark.sql.functions import window, avg, count
from src.shared_utils import get_spark_session

def run_gold_aggregation(silver_path, gold_path):
    spark = get_spark_session("GoldAggregates")

    silver_df = spark.readStream \
        .format("delta") \
        .load(silver_path)

    # AGGREGATION: Average temp per device every 5 minutes
    agg_df = silver_df \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("device_id")
        ) \
        .agg(
            avg("temperature").alias("avg_temp"),
            count("signal_strength").alias("reading_count")
        )

    # Gold tables are often Complete mode (overwriting with latest state)
    query = agg_df.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", f"{gold_path}/_checkpoints") \
        .start(gold_path)

    query.awaitTermination()