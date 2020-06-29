"""
配置Spark通过JDBC连接数据库MySQL，编程实现利用DataFrame插入数据到MySQL，最后打印出age的最大值和age的总和。
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)
# spark实例化的另一类方法
spark = SQLContext(sc)


if __name__ == '__main__':
    # rdd转df
    employeeRDD = sc.parallelize([['3', 'Mary', 'F', '26'], ['4', 'Tom', 'M', '23']])
    row_rdd = employeeRDD.map(lambda line: Row(int(line[0]), line[1], line[2], int(line[3])))

    # 设置模式信息,最后的bool是是否可以为null,这里必须用格式化的方法来生成df,否则写入sql的数据格式会有问题
    schema = StructType([StructField("id", IntegerType(), True),\
                         StructField("name", StringType(), True),\
                         StructField("gender", StringType(), True),\
                         StructField("age", IntegerType(), True)])

    employeeDF = spark.createDataFrame(row_rdd, schema)

    # 写入数据库
    prop = {}
    prop['user'] = 'root'
    prop['password'] = '66117254qn'
    prop['driver'] = 'com.mysql.jdbc.Driver'
    employeeDF.write.jdbc('jdbc:mysql://39.106.181.28:3306/sql_test', 'employee', 'append', prop)


