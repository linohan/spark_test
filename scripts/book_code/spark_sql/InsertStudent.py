"""
向sql数据库写入数据
"""

from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

# 设置模式信息,最后的bool是是否可以为null
schema = StructType([StructField("id", IntegerType(), True),\
                     StructField("name", StringType(), True),\
                     StructField("gender", StringType(), True),\
                     StructField("age", IntegerType(), True)])

# 设置两条数据
studentRDD = spark.sparkContext.parallelize(["3 Rongcheng M 26", "4 Guanhua M 27"]).\
    map(lambda x: x.split(" "))

# 创建Row对象
rowRDD = studentRDD.map(lambda p: Row(int(p[0].strip()), p[1].strip(), p[2].strip(), int(p[3].strip())))

# 建立Row对象和模式之间的对应关系
studentDF = spark.createDataFrame(rowRDD, schema)

# 写入数据库
prop = {}
prop['user'] = 'root'
prop['password'] = '66117254qn'
prop['driver'] = 'com.mysql.jdbc.Driver'
studentDF.write.jdbc('jdbc:mysql://39.106.181.28:3306/sql_test', 'stu', 'append', prop)
