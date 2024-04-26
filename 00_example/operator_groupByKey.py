from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('reduceByKey')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 1),('a', 1), ('b', 2), ('a', 1)])
    result = rdd.groupByKey()
    print(result.map(lambda x:(x[0], list(x[1]))).collect())
    # [('a', [1, 1, 1, 1]), ('b', [2, 2])]