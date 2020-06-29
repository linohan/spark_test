#!/miniconda2/bin/python
# encoding=utf-8
#########################################################################
# File Name: TestPartitioner.py
# Author: Ryoma
# mail: ryomawithlst@gmail.com
# Created Time: 2020年06月22日 星期一 23时16分19秒
#########################################################################
from pyspark import SparkConf, SparkContext


index = 0

def getindex():
	global index
	index += 1
	return index

def main():
	conf = SparkConf().setMaster("local").setAppName("MyApp")
	sc = SparkContext(conf=conf)
	lines = sc.textFile("file:///root/tmp/file_sort/file*.txt")
	index = 0
	result1 = lines.filter(lambda line:(len(line.strip())>0))
	result2 = result1.map(lambda x:(int(x.strip()), ""))
	result3 = result2.repartition(1)
	result4 = result3.sortByKey(True)
	result5 = result4.map(lambda x:x[0])
	result6 = result5.map(lambda x:(getindex(),x))
	result6.foreach(print)


if __name__ == '__main__':
	main()
