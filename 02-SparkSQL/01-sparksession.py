from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('exmaple').getOrCreate()
    sc = spark.sparkContext

    df = spark.read.csv('../data/stu_score.txt', sep=',', header=False)
    df2 = df.toDF('id','name','score')
    # df2.printSchema()
    # df2.show()

    df2.createTempView('score')

    spark.sql('select * from score where name="数学" limit 5').show()