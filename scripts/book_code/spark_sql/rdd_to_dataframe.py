"""
编程实现将RDD转换为DataFrame
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

# ## 方法一：利用反射来推断包含特定类型对象的RDD的schema，适用对已知数据结构的RDD转换；
# 使用rdd读入文档
people_rdd = sc.textFile("../../../data/txt/employee.txt")
# rdd转Row转dataframe
row_df = people_rdd.map(lambda line: line.split(',')).map(lambda attr: Row(int(attr[0]), attr[1], int(attr[2]))).toDF()
# dataframe转sql
row_df.createOrReplaceTempView("employee")
# 使用spark.sql读取sql,得到DataFrame
person_df = spark.sql("select * from employee")
# 格式化打印,这里注意df没有map功能,需要先转换成rdd再map,str之间才能拼接,所以需要把int转换成str
person_df.rdd.map(lambda x: "id:"+str(x[0])+', '+"name:"+x[1]+', '+"age:"+str(x[2])).foreach(print)

# ## 方法二：使用编程接口，构造一个schema并将其应用在已知的RDD上。
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType

# 生成表头
schemaString = "id name age"
# 这里的True是nullable
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
schema = StructType(fields)
# 使用rdd读入文档
employee_rdd = sc.textFile("../../../data/txt/employee.txt")
# 或者:
# people_rdd = spark.sparkContext.textFile("../../../data/txt/employee.txt")

row_rdd = employee_rdd.map(lambda line: line.split(",")).map(lambda line: Row(int(line[0]), line[1], int(line[2])))
row_df = spark.createDataFrame(row_rdd, schema)

# dataframe转sql
row_df.createOrReplaceTempView("employee")
# 使用spark.sql读取sql,得到DataFrame
person_df = spark.sql("select * from employee")
# 格式化打印,这里注意df没有map功能,需要先转换成rdd再map,str之间才能拼接,所以需要把int转换成str
person_df.rdd.map(lambda x: "id:"+str(x[0])+', '+"name:"+x[1]+', '+"age:"+str(x[2])).foreach(print)
