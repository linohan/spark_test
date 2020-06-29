from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf().setMaster("local").setAppName("MyApp")
    sc = SparkContext(conf=conf)
    lines1 = sc.textFile("file:///root/tmp/code/avgscore/Algorithm.txt")
    lines2 = sc.textFile("file:///root/tmp/code/avgscore/Database.txt")
    lines3 = sc.textFile("file:///root/tmp/code/avgscore/Python.txt")

    lines = lines1.union(lines2).union(lines3)
    res = lines.map(lambda x: x.split(" ")).map(lambda x: (x[0], (int(x[1]), 1)))
    sum = res.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    avg = sum.map(lambda x: (x[0], round(x[1][0] / float(x[1][1]), 2)))
    # avg.foreach(print) # 中文打印不了,会报错
    avg.repartition(1).saveAsTextFile("file:///root/tmp/code/avgscore/result")


if __name__ == '__main__':
    main()
