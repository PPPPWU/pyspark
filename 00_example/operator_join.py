from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('map')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(1001,'a'),(1002,'b'),(1003,'a'),(1004,'b')])
    rdd2 = sc.parallelize([(1001,'b'), (1002,'c')])

    print(rdd1.join(rdd2).collect())


    print(rdd1.leftOuterJoin(rdd2).collect())