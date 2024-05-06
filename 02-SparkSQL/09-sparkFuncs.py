from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F


if __name__ == '__main__':
    spark = SparkSession.builder.appName('sparkFunc').getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile('hdfs://node1:8020/input/words.txt').\
        flatMap(lambda x:x.split(' ')).\
        map(lambda x:[x])

    # df = rdd.toDF(['word'])
    # df.createTempView('words')
    # spark.sql('select word, count(*) as cnt from words group by word order by cnt desc').show()

    df = spark.read.format('text').load('hdfs://node1:8020/input/words.txt')
    # df.select(F.explode(F.split(df['value'], ' '))).show()
    df2 = df.withColumn('value', F.explode(F.split(df['value'], ' ')))
    df2.groupBy('value').count().withColumnRenamed('count','cnt').\
        orderBy('cnt', ascending=False).show()