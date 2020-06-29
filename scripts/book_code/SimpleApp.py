#!/miniconda2/bin/python
# encoding=utf-8
#########################################################################
# File Name: SimpleApp.py
# Author: Ryoma
# mail: ryomawithlst@gmail.com
# Created Time: 2020年06月22日 星期一 22时40分50秒
#########################################################################

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)
logFile = "hdfs://localhost:9000/user/hadoop/test.txt"
logData = sc.textFile(logFile)
linecount = logData.count()
print("-"*50)
print(linecount)
print("-"*50)
