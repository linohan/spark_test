#!/miniconda2/bin/python
# encoding=utf-8
#########################################################################
# File Name: TestPartitioner.py
# Author: Ryoma
# mail: ryomawithlst@gmail.com
# Created Time: 2020年06月22日 星期一 23时16分19秒
#########################################################################
from pyspark import SparkConf, SparkContext
from operator import gt

class SecondarySortKey():
	def __init__(self, k):
		self.column1 = k[0]
		self.column2 = k[1]

	def __gt__(self, other):
		if other.column1 == self.column1:
			return gt(self.column2, other.column2)
		else:
			return gt(self.column1, other.column1)


def main():
	conf = SparkConf().setMaster("local").setAppName("MyApp")
	sc = SparkContext(conf=conf)
	lines = sc.textFile("file:///root/tmp/file_sort_2/file4.txt")
	result1 = lines.filter(lambda line:(len(line.strip())>0))
	result2 = result1.map(lambda x:((int(x.split(" ")[0]), int(x.split(" ")[1])),x))
	result3 = result2.map(lambda x:(SecondarySortKey(x[0]),x[1]))
	result4 = result3.sortByKey(False)
	result5 = result4.map(lambda x:x[1])
	# result5.saveAsTextFile("file:///root/tmp/test_sort2")
	result5.foreach(print)

def main1():
	conf = SparkConf().setMaster("local").setAppName("MyApp")
	sc = SparkContext(conf=conf)
	lines = sc.textFile("file:///root/tmp/file_sort_2/file4.txt")
	result1 = lines.filter(lambda line:(len(line.strip())>0))
	result2 = result1.map(lambda x:(int(x.split(" ")[0]), int(x.split(" ")[1])))
	result3 = result2.map(lambda x:(SecondarySortKey(x),""))
	result4 = result3.sortByKey(False)
	result5 = result4.map(lambda x:(x[0].column1,x[0].column2))
	# result5.saveAsTextFile("file:///root/tmp/test_sort2")
	result5.foreach(print)

if __name__ == '__main__':
	main1()
