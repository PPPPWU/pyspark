from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('map')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(['a b c', 'b c d','d e f'])

    print(rdd.flatMap(lambda x:x.split(' ')).collect())