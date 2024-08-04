from pyspark.sql import SparkSession

# Initialize a SparkSession
def get_spark_session (appName, master="local[*]"):
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .getOrCreate()
    return spark