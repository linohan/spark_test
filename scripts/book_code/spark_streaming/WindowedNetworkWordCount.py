"""
使用socket作为数据源,reduceByKeyAndWindow,滑动窗口计数
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
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("./checkpoint")
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    words = lines.flatMap(lambda line: line.split(' '))
    wordCounts = words.map(lambda word: (word, 1)).\
        reduceByKeyAndWindow(lambda x, y: x+y, lambda x, y: x-y, 30, 10)
    wordCounts.pprint()
    ssc.start()
    ssc.awaitTermination()
