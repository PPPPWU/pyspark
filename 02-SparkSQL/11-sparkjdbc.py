import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == '__main__':
    spark = SparkSession.builder.appName('movierv').\
        config('spark.sql.shuffle.partitions','10').getOrCreate()

    schema = StructType().add('user_id', StringType(), nullable=True).\
        add('movie_id', IntegerType(), nullable=True).\
        add('rank', IntegerType(), nullable=True).\
        add('ts', StringType(), nullable=True)

    df = spark.read.format('csv').option('sep', '\t').\
        option('header', False).option('encoding', 'utf-8').\
        schema(schema).load('../data/sql/u.data')

    df.write.mode('overwrite').format('jdbc').\
        option('url','jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true').\
        option('dbtable','u_data').\
        option('user', 'root').\
        option('password', '123456').save()