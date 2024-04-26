from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('reduceByKey')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 1),('a', 1), ('b', 2), ('a', 1)])
    result = rdd.reduceByKey(lambda  a, b : a+b)
    print(result.collect())
    # [('a', 4), ('b', 4)]