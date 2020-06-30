from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pprint import pprint
import time

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

    wordCounts = words.groupBy('word').count()

    # 执行流计算 默认是微批处理模式,trigger－每隔一段时间进行一次计算
    query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .queryName('write_to_console')\
        .trigger(processingTime="8 seconds")\
        .start()

    while True:
        if query.lastProgress:
            if query.lastProgress['numInputRows'] > 0:
                pprint(query.lastProgress)

        pprint(query.status)
        time.sleep(5)
