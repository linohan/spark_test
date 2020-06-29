#!/miniconda2/bin/python
# encoding=utf-8
#########################################################################
# File Name: TestPartitioner.py
# Author: Ryoma
# mail: ryomawithlst@gmail.com
# Created Time: 2020年06月22日 星期一 23时16分19秒
#########################################################################
from pyspark import SparkConf, SparkContext


def main():
	conf = SparkConf().setMaster("local").setAppName("MyApp")
	sc = SparkContext(conf=conf)
	lines = sc.textFile("file:///root/tmp/file0.txt")
	result1 = lines.filter(lambda line:(len(line.strip())>0) and (len(line.split(","))==4))
	result2 = result1.map(lambda x:x.split(",")[2])
	result3 = result2.map(lambda x:(int(x), ""))
	result4 = result3.repartition(1)
	result5 = result4.sortByKey(False)
	result6 = result5.map(lambda x:x[0])
	result7 = result6.take(5)
	for a in result7:
		print(a)

if __name__ == '__main__':
	main()
