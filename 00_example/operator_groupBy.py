from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('map')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5])
    rdd2 = rdd.groupBy(lambda num: 'even' if(num%2==0) else 'odd')

    print(rdd2.map(lambda x:(x[0], list(x[1]))).collect())