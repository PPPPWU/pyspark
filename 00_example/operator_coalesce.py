from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('partitionby')
    sc = SparkContext(conf=conf)


    rdd =sc.parallelize([('hadoop',1),('hello',1),('flink',1),('hadoop',1),('spark',1)])
    print(rdd.coalesce(3, shuffle=True).glom().collect())