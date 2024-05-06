import string
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import pandas as pd

if __name__ == '__main__':
    spark = SparkSession.builder.appName('movierv').\
        config('spark.sql.shuffle.partitions','10').getOrCreate()
    sc= spark.sparkContext

    rdd = sc.parallelize([
        ('张三', 'class_1', 99),
        ('王五', 'class_2', 35),
        ('王三', 'class_3', 57),
        ('王久', 'class_4', 12),
        ('王丽', 'class_5', 99),
        ('王娟', 'class_1', 90),
        ('王军', 'class_2', 91),
        ('王俊', 'class_3', 33),
        ('王君', 'class_4', 55),
        ('王珺', 'class_5', 66),
        ('郑颖', 'class_1', 11),
        ('郑辉', 'class_2', 33),
        ('张丽', 'class_3', 36),
        ('张张', 'class_4', 79),
        ('黄凯', 'class_5', 90),
        ('黄开', 'class_1', 90),
        ('黄恺', 'class_2', 90),
        ('王凯', 'class_3', 11),
        ('王凯杰', 'class_1', 11),
        ('王开杰', 'class_2', 3),
        ('王景亮', 'class_3', 99)
    ])
    schema = StructType().add("name", StringType()). \
        add("class", StringType()). \
        add("score", IntegerType())
    df = rdd.toDF(schema)

    df.createTempView('stu')

    spark.sql("""
        SELECT *, AVG(score) OVER(PARTITION BY class) AS avg_score FROM stu
    """).show()