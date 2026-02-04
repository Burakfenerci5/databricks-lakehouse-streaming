from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from src.shared_utils import get_spark_session

def run_bronze_ingestion(source_dir, output_path):
    spark = get_spark_session("BronzeIngestion")
    
    # Define a schema for IoT data (enforcing schema on read is a best practice)
    schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("signal_strength", DoubleType(), True)
    ])

    # READ STREAM: Simulating Auto Loader
    raw_stream = spark.readStream \
        .format("json") \
        .schema(schema) \
        .load(source_dir)

    # WRITE STREAM: Append to Delta Table
    query = raw_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{output_path}/_checkpoints") \
        .start(output_path)
    
    query.awaitTermination()

if __name__ == "__main__":
    # In a real job, these would be arguments
    run_bronze_ingestion("/tmp/landing_zone", "/tmp/delta/bronze")