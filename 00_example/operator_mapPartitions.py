from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('mappartition')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1,10),3)
    def f(iterator): yield sum(iterator)
    print(rdd.mapPartitions(f).collect())