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

    df.createTempView('movie')

    # avg-rank of user
    # df.groupBy('user_id').avg('rank').withColumnRenamed('avg(rank)','avg_rank').\
    #     withColumn('avg_rank', F.round('avg_rank',2)).\
    #     orderBy('avg_rank', ascending=False).show()
    # spark.sql("""select user_id, AVG(rank) as avg from movie group by user_id order by avg desc""").show()

    # avg_rank of movies
    # df.groupBy('movie_id').avg('rank').withColumnRenamed('avg(rank)','avg_score').\
    #     withColumn('avg_score',F.round('avg_score',2)).\
    #     orderBy('avg_score', ascending=False).show()

    # num of movie more than avg rank
    sql = '''
        select count(*)
        from movie
        where rank > (select avg(rank) from movie)
    '''
    spark.sql(sql).show()
    print(df.select(F.avg(df['rank'])).first())
    print(df.where(df['rank']>df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    # sql = """
    #     select t1.user_id, AVG(rank)
    #     from (
    #         select user_id, count(rank) as cnt
    #              from movie where rank>3
    #              group by user_id
    #              order by cnt desc limit 1
    #     ) t1, movie t2
    #     where t1.user_id = t2.user_id
    #     group by t1.user_id
    # """
    # spark.sql(sql).show()
    # user_id = df.where('rank>3').groupBy('user_id').count().withColumnRenamed('count','cnt').\
    #     orderBy('cnt', ascending=False).limit(1).first()['user_id']
    # df.filter(df['user_id'] == user_id).select(F.round(F.avg('rank'),2)).show()

    df.groupBy('user_id').agg(
        F.round(F.avg('rank'),2).alias('avg_rank'),
        F.min('rank').alias('min_rank'),
        F.max('rank').alias('max_rank')
    ).show()
    time.sleep(100)

    df.groupBy('movie_id').agg(
        F.count('movie_id').alias('cnt'),
        F.round(F.avg('rank'),2).alias('avg_rank')
    ).where('cnt>100').orderBy('avg_rank', ascending=False).limit(10).show()
