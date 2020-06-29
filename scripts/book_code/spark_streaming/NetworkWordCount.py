"""
使用socket作为数据源
"""

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: NetworkWordCount.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="NetworkWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    words = lines.flatMap(lambda line: line.split(' '))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    wordCounts.pprint()
    ssc.start()
    ssc.awaitTermination()
