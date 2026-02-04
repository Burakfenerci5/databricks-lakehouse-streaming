from pyspark.sql import SparkSession

def get_spark_session(app_name="LakehouseStreaming"):
    """
    Creates a Spark Session with Delta Lake support enabled.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()