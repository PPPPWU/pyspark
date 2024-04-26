from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('reduceByKey')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 3), ('c', 2),('d', 4), ('e', 2), ('f', 3),('g', 2),('h', 4), ('i', 2), ('j', 3),('k', 2),('l', 4), ('m', 2), ('a', 3)],2)
    result = rdd.sortByKey(ascending=True, numPartitions=6)
    print(result.collect())