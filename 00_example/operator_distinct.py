from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('map')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,3,4,4,5])

    print(rdd.distinct().collect())