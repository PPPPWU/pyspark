import pandas
from pyspark.sql import SparkSession
import pandas as pd

if __name__ == '__main__':
    spark = SparkSession.builder.appName('DFfromPD').getOrCreate()
    sc = spark.sparkContext

    pdf = pd.DataFrame({
        'id':[1,2,3],
        'name':['a','b','c'],
        'age':[12,13,14]
    })

    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()

