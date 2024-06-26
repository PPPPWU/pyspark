from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName('sparkdfbuild').getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile('../data/sql/people.txt').map(lambda x: x.split(',')).map(lambda x: (x[0], int(x[1])))
    schema = StructType().add('name',StringType(),nullable=True).add('age',IntegerType(),nullable=True)
    df = spark.createDataFrame(rdd, schema=schema)
    df.printSchema()
    df.show()