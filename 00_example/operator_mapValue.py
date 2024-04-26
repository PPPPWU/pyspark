from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('partitionby')
    sc = SparkContext(conf=conf)


    rdd =sc.parallelize([('hadoop',1),('hello',4),('flink',2),('hadoop',5),('spark',6)])
    print(rdd.mapValues(lambda x: x+10).collect())

