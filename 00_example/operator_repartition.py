from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('partitionby')
    sc = SparkContext(conf=conf)


    rdd =sc.parallelize([('hadoop',1),('hello',1),('flink',1),('hadoop',1),('spark',1)])
    rdd2 = rdd.repartition(2)
    print(rdd2.glom().collect())
    rdd3 = rdd.repartition(5)
    print(rdd3.glom().collect())