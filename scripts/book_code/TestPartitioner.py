#!/miniconda2/bin/python
# encoding=utf-8
#########################################################################
# File Name: TestPartitioner.py
# Author: Ryoma
# mail: ryomawithlst@gmail.com
# Created Time: 2020年06月22日 星期一 23时16分19秒
#########################################################################
from pyspark import SparkConf, SparkContext


def MyPartitioner(key):
	print('the key is %d'%key)
	return key%10


def main():
	conf = SparkConf().setMaster("local").setAppName("MyApp")
	sc = SparkContext(conf=conf)
	data = sc.parallelize(range(10),5)
	data.map(lambda x:(x,1)).partitionBy(10, MyPartitioner).map(lambda x:x[0]).saveAsTextFile("file:///root/tmp/code/rdd/partitioner")


if __name__ == '__main__':
	main()
