from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import length

if __name__ == '__main__':
    spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # 创建输入数据源
    lines = spark\
        .readStream\
        .format('socket')\
        .option('host', 'localhost')\
        .option('port', 9999)\
        .load()

    # 定义流计算过程
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    all_length_5_words = words.filter(length("word") == 5)

    # 执行流计算 默认是微批处理模式,trigger－每隔一段时间进行一次计算
    query = all_length_5_words\
        .writeStream\
        .outputMode('append')\
        .format('parquet')\
        .option('path', './tmp/fliesink')\
        .option('checkpointLocation', './tmp/file-sink-cp')\
        .trigger(processingTime="8 seconds")\
        .start()

    query.awaitTermination()
