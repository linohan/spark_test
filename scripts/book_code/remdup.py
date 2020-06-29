from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf().setMaster("local").setAppName("MyApp")
    sc = SparkContext(conf=conf)
    lines1 = sc.textFile("file:///root/tmp/code/remdup/A")
    lines2 = sc.textFile("file:///root/tmp/code/remdup/B")
    lines = lines1.union(lines2)
    distinct_lines = lines.distinct()
    res = distinct_lines.sortBy(lambda x:x)
    res.repartition(1).saveAsTextFile("file:///root/tmp/code/remdup/result")


if __name__ == '__main__':
    main()
