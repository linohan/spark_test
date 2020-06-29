"""
编程实现利用DataFrame读写MySQL的数据
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()


if __name__ == '__main__':
    df = spark.read.format("jdbc").\
        option("driver", "com.mysql.jdbc.Driver"). \
        option("url", "jdbc:mysql://39.106.181.28:3306/sql_test"). \
        option("dbtable", "stu").\
        option("user", "root"). \
        option("password", "66117254qn").\
        load()

    df.show()
