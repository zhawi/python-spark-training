from sparkSession.sparksession import get_spark_session
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
    spark = get_spark_session("mnmcountapp")

    #get the MnM file from resources folder passed as an argument for the app
    mnmfile = sys.argv[1]

    #read the file into a spark dataframe using the CSV
    #infering schema to facilitate the DF creation, file has header
    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnmfile)
              )
    
    count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy("Sum(Count)", asceding=False)
        )
    
    mnm_df.show(60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    ca_count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .sum("Count")
        )
    
    ca_count_mnm_df.show(10, truncate=False)

    spark.stop()