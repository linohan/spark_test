from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("TestRateStreamSource") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    lines = spark\
        .readStream\
        .format('rate')\
        .option('rowsPerSecond', 5)\
        .load()

    print(lines.schema)

    query = lines\
        .writeStream\
        .outputMode('update')\
        .format('console')\
        .option('truncate', 'false')\
        .start()

    query.awaitTermination()
