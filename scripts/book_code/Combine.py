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
	data = sc.parallelize([("company-1",88),("company-1",96),("company-1",85),\
						   ("company-2",94),("company-2",86),("company-2",74),\
						   ("company-3",86),("company-3",88),("company-3",92)],3)
	res = data.combineByKey(\
			lambda income:(income,1),\
			lambda acc,income:(acc[0]+income,acc[1]+1),\
			lambda acc1,acc2:(acc1[0]+acc2[0],acc1[1]+acc2[1])).\
			map(lambda x:(x[0],x[1][0],x[1][0]/float(x[1][1])))
	res.repartition(1).saveAsTextFile("file:///root/tmp/code/rdd/combine")
	#res.saveAsTextFile("file:///root/tmp/code/rdd/combine")


if __name__ == '__main__':
	main()
