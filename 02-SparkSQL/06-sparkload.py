from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName('sparkload').getOrCreate()
    sc = spark.sparkContext

    # load txt file
    # schema = StructType().add("data", StringType(), nullable=True)
    # df = spark.read.format('text').schema(schema).load('../data/sql/people.txt')
    # df.printSchema()
    # df.show()

    # load json file
    # df = spark.read.format('json').load('../data/sql/people.json')
    # df.printSchema()
    # df.show()

    # load csv file
    # df = spark.read.format('csv').option('sep',';').option('header',True).option('encoding','utf-8').\
    #     schema('name STRING, age INT, job STRING').load('../data/sql/people.csv')
    # df.printSchema()
    # df.show()

    # load parquet
    df = spark.read.format('parquet').load('../data/sql/users.parquet')
    df.printSchema()
    df.show()