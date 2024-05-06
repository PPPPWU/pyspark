from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


if __name__ == '__main__':
    spark = SparkSession.builder.appName('SQL').getOrCreate()
    sc = spark.sparkContext

    # df = spark.read.format('parquet').load('../data/sql/users.parquet')
    # df.select('name').show()

    rdd = sc.textFile('../data/sql/stu_score.txt').map(lambda x:x.split(',')).map(lambda x:(x[0], x[1], int(x[2])))
    schema = StructType().add('id', StringType(), nullable=True).add('subject', StringType(), nullable=True).add('score', IntegerType(), nullable=True)
    df = spark.createDataFrame(rdd, schema)

    df.createTempView('score')
    spark.sql('select * from score where score = 95').show()