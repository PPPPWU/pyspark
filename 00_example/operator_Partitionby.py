from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('partitionby')
    sc = SparkContext(conf=conf)


    rdd =sc.parallelize([('hadoop',1),('hello',1),('flink',1),('hadoop',1),('spark',1)])
    def partition_self(key):
        if 'hadoop' == key:
            return 0
        elif('spark' == key or 'flink' == key):
            return 1
        else:
            return 2

    print(rdd.partitionBy(3, partition_self).glom().collect())