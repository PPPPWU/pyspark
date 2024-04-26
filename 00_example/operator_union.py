from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('map')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1,2,3,4,5], 3)
    rdd2 = sc.parallelize(['a', 'b', 'c'])

    print(rdd1.getNumPartitions())
    print(rdd1.union(rdd2).collect())