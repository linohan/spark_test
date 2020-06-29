"""
Spark Streaming基本代码
"""

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
conf = SparkConf()
conf.setAppName('TestDStream')
conf.setMaster('local[2]')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)  # 10代表每10秒数据切成一分段
lines = ssc.textFileStream('')
