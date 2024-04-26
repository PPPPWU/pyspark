from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('map')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,2,3,4,5])
    def map_fun(data):
        return data * 10

    print(rdd.map(map_fun).collect())