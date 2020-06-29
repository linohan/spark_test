"""
Spark SQL基本操作
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

print('创建DataFrame')
employee_df = spark.read.json("../../../data/json/employee.json")

print('查询DataFrame所有数据')
employee_df.show()

print('去重')
employee_df.distinct().show()

print('查询所有数据,打印时去除id字段')
employee_df.drop('id').show()

print('筛选age>20的记录')
employee_df.filter(employee_df['age']>20).show()

print('将数据按name分组')
employee_df.groupBy('name').count().show()

print('将数据按name升序排列')  # 空格影响排列顺序,怎么处理?
employee_df.sort(employee_df['name'].asc()).show()

print('将数据按name降序排列')
employee_df.sort(employee_df['name'].desc()).show()

print('取出前3行数据')
print(employee_df.take(3))
print(employee_df.head(3))

print('查询所有记录的name列，并为其取别名为username')
employee_df.select(employee_df.name.alias('username')).show()

print('查询年龄age的平均值')
employee_df.agg({'age': 'mean'}).show()

print('查询年龄age的最大值')
employee_df.agg({'age': 'max'}).show()
