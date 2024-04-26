from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('map')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5,6],2)
    print(rdd.glom().collect())