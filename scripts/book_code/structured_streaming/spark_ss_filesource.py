import os
import shutil
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, asc
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import TimestampType, StringType

TEST_DATA_DIR_SPARK = './tmp/testdata/'


if __name__ == "__main__":
    # 定义模式，为时间戳类型的eventTime、字符串类型的操作和省份组成
    schema = StructType([
        StructField('eventTime', TimestampType(), True),
        StructField('action', StringType(), True),
        StructField('district', StringType(), True)
    ])

    spark = SparkSession \
        .builder \
        .appName("StructuredEMallPurchaseCount") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    lines = spark\
        .readStream\
        .format('json')\
        .schema(schema)\
        .option('maxFilesPerTrigger', 100)\
        .load(TEST_DATA_DIR_SPARK)

    # 定义窗口
    windowDuration = '1 minutes'

    windowedCounts = lines\
        .filter("action='purchase'")\
        .groupBy('district', window('eventTime', windowDuration))\
        .count()\
        .sort(asc('window'))

    query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate', 'false')\
        .trigger(processingTime='10 seconds')\
        .start()

    query.awaitTermination()
