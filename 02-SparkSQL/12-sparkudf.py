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

    # rdd = sc.parallelize([1,2,3,4,5,6]).map(lambda x:[x])
    # df = rdd.toDF(['num'])
    #
    # def num_10(data):
    #     return data*10
    # udf1 = spark.udf.register('udf1', num_10, IntegerType())
    #
    # df.selectExpr('udf1(num)').show()
    # df.select(udf1(df['num'])).show()
    #
    # udf2 = F.udf(num_10, IntegerType())
    # df.select(udf2(df['num'])).show()

    # rdd = sc.parallelize([['hadoop spark flink'], ['hadoop flink java']])
    # df = rdd.toDF(['line'])
    #
    # def split_line(data):
    #     return data.split(' ')
    #
    # udf1 = spark.udf.register('udf1', split_line, ArrayType(StringType()))
    #
    # df.select(udf1(df['line'])).show()

    rdd = sc.parallelize([[1],[2],[3]])
    df = rdd.toDF(['num'])

    def split_line(data):
        return {'num':data, 'letter_str':string.ascii_letters[data]}

    struct_type = StructType().add('num', IntegerType(), nullable=True).\
        add('letter_str', StringType(), nullable=True)
    udf1 = spark.udf.register('udf1', split_line, struct_type)
    df.select(udf1(df['num'])).show()
