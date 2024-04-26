from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('fold')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1,10),3)
    print(rdd.fold(10,lambda a, b:a+b))
    print(rdd.first())
    print(rdd.take(5))
    print(rdd.top(3))
    print(rdd.count())
    print(rdd.takeSample(True,8))
    print(rdd.takeOrdered(3))
    print(rdd.foreach(lambda x:print(x*10)))
    rdd.saveAsTextFile('../data/output111.txt')