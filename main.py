from sparkSession.sparksession import get_spark_session

spark = get_spark_session()

strings = spark.read.text("./README.md")
strings.show(10, truncate=False)

strings.count()

spark.stop()