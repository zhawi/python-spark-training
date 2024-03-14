from pyspark.sql import SparkSession

# Initialize a SparkSession
def get_spark_session (appName="Spark Training Session", master="local[*]"):
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()
    return spark