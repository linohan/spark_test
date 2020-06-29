"""
使用socket作为数据源,updateStateByKey,累积计数,写入到文件
"""

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: NetworkWordCount.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="WindowedNetworkWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("./stateful")
    # 初始化RDD
    initialStateRDD = sc.parallelize([(u'hello', 1), (u'world', 1)])

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    words = lines.flatMap(lambda line: line.split(' '))
    wordCounts = words.map(lambda word: (word, 1)).\
        updateStateByKey(updateFunc=updateFunc, initialRDD=initialStateRDD)
    wordCounts.saveAsTextFiles("./stateful/output")
    wordCounts.pprint()
    ssc.start()
    ssc.awaitTermination()
