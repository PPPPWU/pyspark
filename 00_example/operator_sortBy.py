from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('reduceByKey')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 3), ('a', 2),('a', 4), ('b', 2), ('a', 3)],2)
    result = rdd.sortBy(lambda x:x[1], ascending=True, numPartitions=2)
    print(result.collect())